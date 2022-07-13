package quorumIndexer

import (
	"crypto/sha256"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/emitter/ancestor"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

const (
	// DecimalUnit is used to define ratios with integers, it's 1.0
	DecimalUnit = 1e6
	maxVal      = math.MaxUint64/uint64(DecimalUnit) - 1
)

func TestQI(t *testing.T) {
	// numNodes := 70
	nodes := [10]int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	// nodes := [1]int{20}
	for _, numNodes := range nodes {

		weights := make([]pos.Weight, numNodes)
		var w pos.Weight = 1
		for i := range weights {
			weights[i] = w
			// w++ // for non equal stake
		}
		testQI(t, weights)
	}
}

func testQI(t *testing.T, weights []pos.Weight) {
	eventCount := 50
	parentCount := [5]int{2, 4, 6, 8, 10}
	// parentCount := [1]int{4}

	meanDelay := 200
	stdDelay := 50
	maxDelay := meanDelay + 3*stdDelay + 1
	eventInterval := make([]int, len(weights))
	for i := range eventInterval {
		eventInterval[i] = 500 // this determines how many milliseconds the node waits before creating a new event
	}
	for i := range parentCount {
		start := time.Now()
		// // testQuorumIndexer(t, weights, eventCount, parentCount, true)
		testQuorumIndexerLatency(t, weights, eventCount, parentCount[i], true, maxDelay, meanDelay, stdDelay, eventInterval)
		elapsed := time.Since(start)
		fmt.Println("NEW took ", elapsed)

		start = time.Now()
		// testQuorumIndexer(t, weights, eventCount, parentCount, false)
		testQuorumIndexerLatency(t, weights, eventCount, parentCount[i], false, maxDelay, meanDelay, stdDelay, eventInterval)
		elapsed = time.Since(start)
		fmt.Println("OLD took ", elapsed)
	}
}

func testQuorumIndexer(t *testing.T, weights []pos.Weight, eventCount int, parentCount int, newQI bool) {
	// assertar := assert.New(t)

	var maxFrame idx.Frame = 0
	nodes := tdag.GenNodes(len(weights))

	validators := pos.ArrayToValidators(nodes, weights)

	diffMetricFn := func(median, current, update idx.Event, validatorIdx idx.Validator) ancestor.Metric {
		return 0
	}

	var input *abft.EventStore
	var lch *abft.TestLachesis
	inputs := make([]abft.EventStore, len(nodes))
	lchs := make([]abft.TestLachesis, len(nodes))
	quorumIndexers := make([]ancestor.QuorumIndexer, len(nodes))
	for i := 0; i < len(nodes); i++ {
		lch, _, input = abft.FakeLachesis(nodes, weights)
		lchs[i] = *lch
		inputs[i] = *input
		quorumIndexers[i] = *ancestor.NewQuorumIndexer(validators, lch.DagIndex, diffMetricFn, lch.Lachesis)
	}

	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}

	// frame1Events := 0
	// toggle := true

	var epoch idx.Epoch = 1

	nodeCount := len(nodes)
	events := make(map[idx.ValidatorID]dag.Events, nodeCount)
	var headsAll dag.Events

	frameChange := make([]int, nodeCount*eventCount)
	rand.Seed(0) // use a fixed seed for comparison between runs
	// eventsComplete := 0
	// make events
	for i := 0; i < eventCount; i++ {
		// for i := 0; i < nodeCount*eventCount; i++ {

		// self := i % nodeCount
		// if self == 0 {
		// fmt.Println("Events Complete: ", eventsComplete)
		// 	eventsComplete++
		// }
		for k, self := range rand.Perm(nodeCount) {
			// if !(i > eventCount/2 && self > len(nodes)*2/3) {

			creator := nodes[self]
			// make
			e := &tdag.TestEvent{}
			e.SetCreator(creator)
			e.SetParents(hash.Events{}) // first parent is empty hash
			var parent dag.Event
			if ee := events[creator]; len(ee) > 0 {
				parent = ee[len(ee)-1] // first parent is creator's previous event, if it exists
			}
			if parent == nil { // leaf event
				e.SetSeq(1)
				e.SetLamport(1)
			} else { // normal event
				e.SetSeq(parent.Seq() + 1)
				e.AddParent(parent.ID()) // add previous self event as parent
				e.SetLamport(parent.Lamport() + 1)

				// create a list of heads to choose parents from
				// for node, _ := range events {
				// 	if node != creator { // already have self parent so exclude it here
				// 		if ee := events[node]; len(ee) > 0 {
				// 			headsAll = append(headsAll, ee[len(ee)-1])
				// 		}
				// 	}
				// }

				// select only 25% of heads
				var heads dag.Events
				for n, head := range rand.Perm(len(headsAll)) {
					heads = append(heads, headsAll[head])
					if n > len(headsAll)/4 {
						break
					}
				}
				heads = headsAll
				// fmt.Println("Heads complete")
				// choose parents using quorumIndexer
				quorumIndexers[self].SelfParentEvent = parent.ID()
				var parents dag.Events
				for j := 0; j < parentCount-1; j++ {

					var best int
					if newQI {
						best = quorumIndexers[self].Choose(parents.IDs(), heads.IDs()) //new quorumIndexer
					} else {
						best = quorumIndexers[self].SearchStrategy().Choose(parents.IDs(), heads.IDs()) //old quorumIndexer
					}
					parents = append(parents, heads[best])
					// remove chosen head from options
					heads[best] = heads[len(heads)-1]
					heads = heads[:len(heads)-1]
					if len(heads) <= 0 {
						break
					}
				}
				// fmt.Println("Parents Selection complete")
				// add parents to new event

				for _, parent = range parents {
					e.AddParent(parent.ID())
					if e.Lamport() <= parent.Lamport() {
						e.SetLamport(parent.Lamport() + 1)
					}
				}
				// quorumIndexers[self].GetTimingMetric(parents.IDs())
				// fmt.Println("Timing metric: ", metric)
			}
			e.SetEpoch(epoch)

			e.Name = fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator]))
			// save and name event
			hasher := sha256.New()
			hasher.Write(e.Bytes())
			var id [24]byte
			copy(id[:], hasher.Sum(nil)[:24])
			e.SetID(id)
			hash.SetEventName(e.ID(), fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator])))
			events[creator] = append(events[creator], e)
			updateHeads(e, &headsAll)

			for j := 0; j < len(nodes); j++ {

				inputs[j].SetEvent(e)
				lchs[j].DagIndexer.Add(e)

				lchs[j].Lachesis.Build(e)
				lchs[j].Lachesis.Process(e)

				lchs[j].DagIndexer.Flush()
				if !newQI {
					quorumIndexers[j].ProcessEvent(&e.BaseEvent, self == i)

				}
			}
			// fmt.Println("Lachesis Updated")
			// fmt.Println("Event: ", i, " ", e.ID(), " Frame: ", lchs[0].dagIndexer.GetEvent(e.ID()).Frame())
			if lchs[0].DagIndexer.GetEvent(e.ID()).Frame() > maxFrame {
				maxFrame = lchs[0].DagIndexer.GetEvent(e.ID()).Frame()
				// frameChange[i] = 1
				frameChange[i*len(nodes)+k] = 1
			} else {
				// frameChange[i] = 0
				frameChange[i*len(nodes)+k] = 0
			}

			// if maxFrame == 2 && toggle {
			// 	toggle = false
			// 	frame1Events = i
			// 	fmt.Println("Number events for frame 1: ", i)
			// }
			// if maxFrame == 3 {
			// 	fmt.Println("Number events for frame 2: ", i-frame1Events)
			// 	break
		}
	}
	// }
	fmt.Println("")
	if newQI {
		fmt.Println("Test using NEW quorumIndexer")
	} else {
		fmt.Println("Test using OLD quorumIndexer")
	}
	fmt.Println("Max Frame: ", maxFrame)
	fmt.Println("Number of nodes: ", nodeCount)
	fmt.Println("Number of Events: ", eventCount)
	fmt.Println("Max Parents:", parentCount)
	// fmt.Println("Frame Change:")
	// for _, frame := range frameChange {
	// 	fmt.Print(frame, ", ")
	// }
	fmt.Println("")
}

func testQuorumIndexerLatency(t *testing.T, weights []pos.Weight, eventCount int, parentCount int, newQI bool, maxDelay int, meanDelay int, stdDelay int, eventInterval []int) {
	randSrc := rand.New(rand.NewSource(0))  // use a fixed seed of 0 for comparison between runs
	delayRNG := rand.New(rand.NewSource(0)) // use a fixed seed of 0 for comparison between runs
	delayDist := delayCumDist()
	maxDelay = len(delayDist) + 1

	// create a 3D slice with coordinates [time][node][node] that is used to store delayed transmission of events between nodes
	//each time coordinate corresponds to 1 millisecond of delay between a pair of nodes
	eventPropagation := make([][][]*tdag.TestEvent, maxDelay)
	for i := range eventPropagation {
		eventPropagation[i] = make([][]*tdag.TestEvent, len(weights))
		for j := range eventPropagation[i] {
			eventPropagation[i][j] = make([]*tdag.TestEvent, len(weights))
		}
	}

	// create a 2D slice with coordinates [node][node] that is used to store the delays/latencies in milliseconds between pairs of nodes
	delays := make([][]int, len(weights))
	for i := range delays {
		delays[i] = make([]int, len(weights))
	}

	// generate random delays between pairs of nodes
	for i := range delays {
		for j := range delays[i] {
			if i == j {
				delays[i][j] = 1 // send immeadiately to self
			} else {
				delays[i][j] = int(randSrc.NormFloat64()*float64(stdDelay)) + meanDelay
				if delays[i][j] >= maxDelay {
					delays[i][j] = maxDelay - 1
				}
				if delays[i][j] < 1 {
					delays[i][j] = 1
				}
			}
		}
	}

	// setup event creation values
	eventCreationCounter := make([]int, len(weights))
	eventCreationInterval := make([]int, len(weights))
	// slowestNode := 0
	longestInterval := 0

	for i := range eventCreationCounter {
		eventCreationCounter[i] = randSrc.Intn(eventInterval[i])
		eventCreationInterval[i] = eventInterval[i]
		if eventCreationInterval[i] > longestInterval {
			// slowestNode = i
			longestInterval = eventCreationInterval[i]
		}
	}

	// create a list of heads for each node
	headsAll := make([]dag.Events, len(weights))

	var maxFrame idx.Frame = 0

	//setup nodes
	nodes := tdag.GenNodes(len(weights))

	validators := pos.ArrayToValidators(nodes, weights)

	diffMetricFn := func(median, current, update idx.Event, validatorIdx idx.Validator) ancestor.Metric {
		return 0
	}

	var input *abft.EventStore
	var lch *abft.TestLachesis
	inputs := make([]abft.EventStore, len(nodes))
	lchs := make([]abft.TestLachesis, len(nodes))
	quorumIndexers := make([]ancestor.QuorumIndexer, len(nodes))
	for i := 0; i < len(nodes); i++ {
		lch, _, input = abft.FakeLachesis(nodes, weights)
		lchs[i] = *lch
		inputs[i] = *input
		quorumIndexers[i] = *ancestor.NewQuorumIndexer(validators, lch.DagIndex, diffMetricFn, lch.Lachesis)
	}

	// Check if attempting to have more parents than nodes
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	tooFewHeadsCtr := 0

	var epoch idx.Epoch = 1

	nodeCount := len(nodes)
	bufferedEvents := make([]tdag.TestEvents, len(nodes))

	rand.Seed(0) // re-seed for fixed permutations between runs
	eventsComplete := 0
	isLeaf := make([]bool, len(nodes))
	for node := range isLeaf {
		isLeaf[node] = true
	}
	selfParent := make([]tdag.TestEvent, len(nodes))

	timeIdx := maxDelay - 1
	time := -1
	// for eventsComplete < eventCount {
	for time < 60000 { // 60 000= 1 minute simulation
		// move forward one timestep
		timeIdx = (timeIdx + 1) % maxDelay
		time++
		// Check to see if new events are received by nodes
		// if they are, do the appropriate updates for the received event
		for sendNode := 0; sendNode < nodeCount; sendNode++ {
			for receiveNode := 0; receiveNode < nodeCount; receiveNode++ {

				if eventPropagation[timeIdx][sendNode][receiveNode] != nil {

					// there is an event to receive
					e := eventPropagation[timeIdx][sendNode][receiveNode]
					eventPropagation[timeIdx][sendNode][receiveNode] = nil //clear the event from buffer

					//add new event to buffer
					bufferedEvents[receiveNode] = append(bufferedEvents[receiveNode], e)

					// it is required that all of an event's parents have been received before adding to DAG
					//loop through buffer to check for events that can be processed
					for i := len(bufferedEvents[receiveNode]) - 1; i >= 0; i-- {
						buffEvent := bufferedEvents[receiveNode][i]
						process := true
						//check if all parents are in the DAG
						for _, parent := range buffEvent.Parents() {
							if lchs[receiveNode].DagIndexer.GetEvent(parent) == nil {
								// a parent is not yet in the DAG, so don't process this event yet
								process = false
								break
							}
						}
						if process {
							// buffered event now has all parents in the DAG and can now be processed
							frame := processEvent(inputs[receiveNode], &lchs[receiveNode], buffEvent, quorumIndexers[receiveNode], newQI, &headsAll[receiveNode], nodes[receiveNode])
							if frame > maxFrame {
								maxFrame = frame
							}

							//remove the processed event from the buffer
							l := len(bufferedEvents[receiveNode])
							bufferedEvents[receiveNode][i] = bufferedEvents[receiveNode][l-1]
							bufferedEvents[receiveNode] = bufferedEvents[receiveNode][:l-1]

							i = len(bufferedEvents[receiveNode]) - 1 // restart at the (new) end of the buffer, processing this event may make other events processable

						}
					}

				}
			}

		}

		//nodes now create events, when they are ready
		for self := 0; self < nodeCount; self++ {
			eventCreationCounter[self]++

			if eventCreationCounter[self] >= eventCreationInterval[self] {
				//self node is ready to create a new event

				creator := nodes[self]
				e := &tdag.TestEvent{}
				e.SetCreator(creator)
				e.SetParents(hash.Events{}) // first parent is empty hash
				var parents dag.Events
				if isLeaf[self] { // leaf event
					isLeaf[self] = false
					e.SetSeq(1)
					e.SetLamport(1)
				} else { // normal event

					heads := headsAll[self]
					for i := range heads {
						if &selfParent[self].BaseEvent == heads[i] {
							// remove the self parent from options, it is always selected
							heads[i] = heads[len(heads)-1]
							heads = heads[:len(heads)-1]
						}
					}

					e.SetSeq(selfParent[self].Seq() + 1)
					e.AddParent(selfParent[self].ID()) // add previous self event as parent
					e.SetLamport(selfParent[self].Lamport() + 1)

					// choose parents using quorumIndexer
					quorumIndexers[self].SelfParentEvent = selfParent[self].ID()

					if len(heads) <= parentCount-1 {
						tooFewHeadsCtr++ // parent selection method doesn't matter in this case, count occurances
					}

					//add a random head
					// randHead := rand.Intn(len(heads))
					// parents = append(parents, heads[randHead])
					// // remove the random head from options
					// heads[randHead] = heads[len(heads)-1]
					// heads = heads[:len(heads)-1]
					if len(heads) >= 1 {
						// need at least one head to select parents

						for j := 0; j < parentCount-1; j++ {

							var best int
							if newQI {
								best = quorumIndexers[self].Choose(parents.IDs(), heads.IDs()) //new quorumIndexer
							} else {
								best = quorumIndexers[self].SearchStrategy().Choose(parents.IDs(), heads.IDs()) //old quorumIndexer
								// best = rand.Intn(len(heads)) // use this to compare to randomly selecting heads
							}
							parents = append(parents, heads[best])
							// remove chosen head from options
							heads[best] = heads[len(heads)-1]
							heads = heads[:len(heads)-1]
							if len(heads) <= 0 {
								break
							}
						}
					}

					// add parents to new event

					for _, parent := range parents {
						e.AddParent(parent.ID())
						if e.Lamport() <= parent.Lamport() {
							e.SetLamport(parent.Lamport() + 1)
						}
					}
					// quorumIndexers[self].GetTimingMetric(parents.IDs())
					// fmt.Println("Timing metric: ", metric)
				}
				if len(parents) >= 1 || e.Seq() == 1 { // create the event only if it has enough parents, or is a leaf
					e.SetEpoch(epoch)
					e.Seq()
					e.Name = fmt.Sprintf("%s%03d", string('a'+rune(self)), e.Seq())
					// save and name event
					hasher := sha256.New()
					hasher.Write(e.Bytes())
					var id [24]byte
					copy(id[:], hasher.Sum(nil)[:24])
					e.SetID(id)
					hash.SetEventName(e.ID(), fmt.Sprintf("%s%03d", string('a'+rune(self)), e.Seq()))

					selfParent[self] = *e

					//now start propagation of event to other nodes
					delay := 1
					for receiveNode := 0; receiveNode < nodeCount; receiveNode++ {
						// receiveTime := (timeIdx + delays[self][receiveNode]) % maxDelay
						if receiveNode != self {
							delay = sampleDist(delayRNG, delayDist) // get a random delay from data distribution
						} else {
							delay = 1 // no delay to send to self
						}
						receiveTime := (timeIdx + delay) % maxDelay
						eventPropagation[receiveTime][self][receiveNode] = e
					}

					eventCreationCounter[self] = 0 //reset event creation interval counter
					// if self == slowestNode {
					eventsComplete++ // increment count of events created
					// }
				}
			}
		}
	}

	fmt.Println("")
	if newQI {
		fmt.Println("Test using NEW quorumIndexer")
	} else {
		fmt.Println("Test using OLD quorumIndexer")
	}
	fmt.Println("Max Frame: ", maxFrame)
	fmt.Println("Time ", float64(time)/1000)
	fmt.Println("Frames per second: ", (1000*float64(maxFrame))/float64(time))

	fmt.Println("Number of nodes: ", nodeCount)
	fmt.Println("Number of Events: ", eventsComplete)
	fmt.Println("Max Parents:", parentCount)
	fmt.Println("Fraction of events with equal or fewer heads than desired parents: ", float64(tooFewHeadsCtr)/float64(eventsComplete))
}

func updateHeads(newEvent dag.Event, heads *dag.Events) {
	// remove newEvents parents from heads
	for _, parent := range newEvent.Parents() {
		for i := 0; i < len(*heads); i++ {
			if (*heads)[i].ID() == parent {
				(*heads)[i] = (*heads)[len(*heads)-1]
				*heads = (*heads)[:len(*heads)-1]
			}
		}
	}

	*heads = append(*heads, newEvent) //add newEvent to heads
}

func processEvent(input abft.EventStore, lchs *abft.TestLachesis, e *tdag.TestEvent, quorumIndexer ancestor.QuorumIndexer, newQI bool, heads *dag.Events, self idx.ValidatorID) (frame idx.Frame) {
	input.SetEvent(e)

	lchs.DagIndexer.Add(e)
	lchs.Lachesis.Build(e)
	lchs.Lachesis.Process(e)

	lchs.DagIndexer.Flush()
	if !newQI {
		// Old  HighestBefore based quorum indexer needs to process the event
		if self == e.Creator() {
			quorumIndexer.ProcessEvent(&e.BaseEvent, true)
		} else {
			quorumIndexer.ProcessEvent(&e.BaseEvent, false)
		}
	}
	updateHeads(e, heads)
	return lchs.DagIndexer.GetEvent(e.ID()).Frame()
}

func sampleDist(rng *rand.Rand, cumDist []float64) (sample int) {
	// generates a random sample from the distribution used to calculate cumDist (using inverse transform sampling)
	random := rng.Float64()
	for sample = 0; cumDist[sample] <= random && sample < len(cumDist); sample++ {
	}
	return sample
}

func delayCumDist() (cumDist []float64) {
	// the purpose of this function is to caluclate a cumulative distribution of delays for use in creating random samples from the data distribution

	// some delay data in milliseconds
	delayData := [...]int{54, 110, 60, 124, 75, 47, 165, 152, 18, 18, 52, 83, 80, 92, 51, 11, 21, 32, 120, 9, 18, 129, 64, 53, 83, 118, 12, 79, 54, 21, 18, 62, 121, 7, 22, 147, 73, 170, 198, 145, 25, 138, 123, 68, 109, 73, 34, 122, 10, 121, 23, 129, 82, 85, 58, 129, 281, 275, 300, 174, 158, 169, 124, 186, 61, 51, 107, 85, 49, 131, 12, 52, 100, 17, 32, 70, 121, 6, 17, 190, 59, 16, 372, 233, 201, 169, 97, 91, 101, 80, 127, 26, 12, 10, 49, 49, 83, 19, 91, 61, 52, 129, 34, 125, 66, 116, 110, 82, 104, 82, 52, 29, 95, 72, 133, 65, 338, 285, 221, 282, 196, 234, 315, 183, 135, 69, 102, 187, 79, 79, 82, 20, 129, 122, 54, 9, 9, 52, 21, 91, 74, 14, 20, 18, 63, 47, 83, 124, 7, 131, 18, 132, 285, 186, 242, 190, 131, 127, 72, 243, 218, 223, 185, 140, 136, 87, 123, 265, 166, 112, 94, 82, 92, 226, 95, 78, 35, 54, 63, 223, 59, 30, 56, 87, 163, 195, 69, 173, 37, 25, 64, 52, 121, 36, 14, 29, 76, 170, 144, 131, 162, 133, 15, 17, 129, 50, 67, 176, 40, 23, 51, 79, 85, 128, 17, 18, 45, 62, 84, 134, 40, 130, 32, 55, 66, 93, 26, 132, 15, 19, 27, 56, 106, 35, 30, 54, 60, 83, 128, 125, 11, 8, 18, 83, 63, 49, 94, 94, 45, 17, 21, 49, 51, 79, 82, 97, 123, 127, 8, 14, 20, 54, 107, 41, 30, 52, 91, 122, 9, 13, 136, 54, 46, 72, 131, 51, 233, 167, 172, 63, 31, 59, 67, 88, 134, 15, 17, 21, 97, 129, 35, 54, 23, 50, 82, 83, 80, 130, 36, 22, 33, 71, 46, 39, 85, 101, 121, 82, 122, 50, 26, 27, 95, 24, 137, 9, 25, 130, 62, 193, 57, 55, 22, 98}

	// find the maximum delay in the data
	maxDelay := 0
	for _, delay := range delayData {
		if delay > maxDelay {
			maxDelay = delay
		}
	}

	// calculate the distribution of the delay data by diving into 1 millisecond bins
	binVals := make([]float64, maxDelay+1)
	for _, latency := range delayData {
		binVals[latency]++
	}

	//now calculate the cumulative distribution of the delay data
	cumDist = make([]float64, len(binVals))
	cumDist[0] = float64(binVals[0]) / float64(len(delayData))
	npts := float64(len(delayData))
	for i := 1; i < len(cumDist); i++ {
		cumDist[i] = cumDist[i-1] + binVals[i]/npts
	}
	return cumDist
}
