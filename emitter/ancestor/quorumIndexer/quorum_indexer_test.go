package quorumIndexer

import (
	"crypto/sha256"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/Fantom-foundation/go-opera/utils/piecefunc"
	"github.com/Fantom-foundation/go-opera/utils/rate"
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

type emissionTimes struct {
	nowTime     int
	prevTime    int
	minInterval int
	maxInterval int
}

type QITestEvents []*QITestEvent

type QITestEvent struct {
	tdag.TestEvent
	creationTime int
}

func TestQI(t *testing.T) {
	// numNodes := 70
	nodes := [10]int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	// nodes := [1]int{30}
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
	// parentCount := [1]int{3}

	meanDelay := 200
	stdDelay := 50
	maxDelay := meanDelay + 3*stdDelay + 1
	eventInterval := make([]int, len(weights))
	for i := range eventInterval {
		eventInterval[i] = 50 // this determines how many milliseconds the node waits before creating a new event
	}
	var minMetric ancestor.Metric
	minMetric = 0 * DecimalUnit
	// for minMetric <= .5*DecimalUnit {
	fmt.Println("")
	fmt.Println("Min Metric: ", float64(minMetric)/DecimalUnit)
	for i := range parentCount {
		start := time.Now()
		// // testQuorumIndexer(t, weights, eventCount, parentCount, true)
		testQuorumIndexerLatency(t, weights, eventCount, parentCount[i], true, maxDelay, meanDelay, stdDelay, eventInterval, minMetric)
		elapsed := time.Since(start)
		fmt.Println("NEW took ", elapsed)

		start = time.Now()
		// testQuorumIndexer(t, weights, eventCount, parentCount, false)
		testQuorumIndexerLatency(t, weights, eventCount, parentCount[i], false, maxDelay, meanDelay, stdDelay, eventInterval, minMetric)
		elapsed = time.Since(start)
		fmt.Println("OLD took ", elapsed)
	}
	minMetric = minMetric + 0.05*DecimalUnit
	// }
}

func testQuorumIndexerLatency(t *testing.T, weights []pos.Weight, eventCount int, parentCount int, newQI bool, maxDelay int, meanDelay int, stdDelay int, eventInterval []int, minMetric ancestor.Metric) {
	randSrc := rand.New(rand.NewSource(0))  // use a fixed seed of 0 for comparison between runso
	delayRNG := rand.New(rand.NewSource(0)) // use a fixed seed of 0 for comparison between runs
	delayDist := delayCumDist()
	maxDelay = len(delayDist) + 10

	nodeCount := len(weights)

	// create a 3D slice with coordinates [time][node][node] that is used to store delayed transmission of events between nodes
	//each time coordinate corresponds to 1 millisecond of delay between a pair of nodes
	eventPropagation := make([][][][]*QITestEvent, maxDelay)
	for i := range eventPropagation {
		eventPropagation[i] = make([][][]*QITestEvent, nodeCount)
		for j := range eventPropagation[i] {
			eventPropagation[i][j] = make([][]*QITestEvent, nodeCount)
			for k := range eventPropagation[i][j] {
				eventPropagation[i][j][k] = make([]*QITestEvent, 0)
			}
		}
	}

	// create a 2D slice with coordinates [node][node] that is used to store the delays/latencies in milliseconds between pairs of nodes
	delays := make([][]int, nodeCount)
	for i := range delays {
		delays[i] = make([]int, nodeCount)
	}

	// generate random delays between pairs of nodes
	// for i := range delays {
	// 	for j := range delays[i] {
	// 		if i == j {
	// 			delays[i][j] = 1 // send immeadiately to self
	// 		} else {
	// 			delays[i][j] = int(randSrc.NormFloat64()*float64(stdDelay)) + meanDelay
	// 			if delays[i][j] >= maxDelay {
	// 				delays[i][j] = maxDelay - 1
	// 			}
	// 			if delays[i][j] < 1 {
	// 				delays[i][j] = 1
	// 			}
	// 		}
	// 	}
	// }

	// setup event creation values
	eventCreationCounter := make([]int, nodeCount)
	eventCreationInterval := make([]int, nodeCount)
	longestInterval := 0

	for i := range eventCreationCounter {
		eventCreationCounter[i] = randSrc.Intn(eventInterval[i])
		eventCreationInterval[i] = eventInterval[i]
		if eventCreationInterval[i] > longestInterval {
			longestInterval = eventCreationInterval[i]
		}
	}

	// create a list of heads for each node
	headsAll := make([]dag.Events, nodeCount)

	//setup nodes
	nodes := tdag.GenNodes(nodeCount)
	validators := pos.ArrayToValidators(nodes, weights)
	diffMetricFn := func(median, current, update idx.Event, validatorIdx idx.Validator) ancestor.Metric {
		return updMetric(median, current, update, validatorIdx, validators)
	}

	busyRate := rate.NewGauge()
	var input *abft.EventStore
	var lch *abft.TestLachesis
	inputs := make([]abft.EventStore, nodeCount)
	lchs := make([]abft.TestLachesis, nodeCount)
	quorumIndexers := make([]ancestor.QuorumIndexer, nodeCount)
	for i := 0; i < nodeCount; i++ {
		lch, _, input = abft.FakeLachesis(nodes, weights)
		lchs[i] = *lch
		inputs[i] = *input
		quorumIndexers[i] = *ancestor.NewQuorumIndexer(validators, lch.DagIndex, diffMetricFn, lch.Lachesis)
	}

	// Check if attempting to have more parents than nodes
	if parentCount > nodeCount {
		parentCount = nodeCount
	}
	tooFewHeadsCtr := 0
	tooFewHeads := 0

	var epoch idx.Epoch = 1
	var maxFrame idx.Frame = 0

	bufferedEvents := make([]QITestEvents, nodeCount)
	updateEmission := make([]bool, nodeCount)

	rand.Seed(0) // re-seed for fixed permutations between runs
	eventsComplete := 0
	isLeaf := make([]bool, nodeCount)
	for node := range isLeaf {
		isLeaf[node] = true
		updateEmission[node] = true
	}

	// selfParent := make([]tdag.TestEvent, nodeCount)
	selfParent := make([]QITestEvent, nodeCount)
	newEventReceived := make([]pos.WeightCounter, nodeCount)
	for i, _ := range newEventReceived {
		newEventReceived[i] = *validators.NewCounter()
	}

	timeIdx := maxDelay - 1
	time := -1

	// now start the simulation
	for time < 60000 { // 60 000= 1 minute simulation
		// move forward one timestep
		timeIdx = (timeIdx + 1) % maxDelay
		time++
		// Check to see if new events are received by nodes
		// if they are, do the appropriate updates for the received event

		for receiveNode := 0; receiveNode < nodeCount; receiveNode++ {
			// check for events to be received by other nodes (including self)
			for sendNode := 0; sendNode < nodeCount; sendNode++ {
				for i := 0; i < len(eventPropagation[timeIdx][sendNode][receiveNode]); i++ {
					e := eventPropagation[timeIdx][sendNode][receiveNode][i]
					updateEmission[receiveNode] = true // with a new received event, DAG progress may have improved enough to allow event emission, set this flag to check

					//add new event to buffer for cheecking if events are ready to put in DAG
					bufferedEvents[receiveNode] = append(bufferedEvents[receiveNode], e)
				}
				//clear the events
				eventPropagation[timeIdx][sendNode][receiveNode] = eventPropagation[timeIdx][sendNode][receiveNode][:0]
			}
			// it is required that all of an event's parents have been received before adding to DAG
			// loop through buffer to check for events that can be processed
			for i := len(bufferedEvents[receiveNode]) - 1; i >= 0; i-- {
				// for i := 0; i < len(bufferedEvents[receiveNode]); i++ {

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

					i = l - 1 // Note the for loop postcondition i-- gives last element. Restart from the start of the buffer, processing this event may make other events processable

					// keep a count of events created after self's most recent event was created
					if buffEvent.creationTime > selfParent[receiveNode].creationTime {
						newEventReceived[receiveNode].Count(buffEvent.Creator())
					}
				}
			}
		}

		//nodes now create events, when they are ready
		for self := 0; self < nodeCount; self++ {
			eventCreationCounter[self]++

			if (eventCreationCounter[self] >= eventCreationInterval[self] && updateEmission[self]) || eventCreationCounter[self] >= 1500 {
				//self node is ready to try creating a new event

				creator := nodes[self]
				// e := &tdag.TestEvent{}
				e := &QITestEvent{}
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
						tooFewHeads = 1 // parent selection method doesn't matter in this case, count occurances
					} else {
						tooFewHeads = 0
					}

					if len(heads) >= 1 {
						// need at least one head to select parents

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
					// e.Seq()
					// e.Name = fmt.Sprintf("%s%03d", string('a'+rune(self)), e.Seq())
					e.Name = fmt.Sprintf("%03d%04d", self, e.Seq())
					// save and name event
					hasher := sha256.New()
					hasher.Write(e.Bytes())
					var id [24]byte
					copy(id[:], hasher.Sum(nil)[:24])
					e.SetID(id)
					// hash.SetEventName(e.ID(), fmt.Sprintf("%s%03d", string('a'+rune(self)), e.Seq()))
					hash.SetEventName(e.ID(), fmt.Sprintf("%03d%04d", self, e.Seq()))

					e.creationTime = time

					var eTimes emissionTimes
					eTimes.nowTime = time
					eTimes.prevTime = selfParent[self].creationTime
					eTimes.minInterval = eventCreationInterval[self]
					eTimes.maxInterval = 2 * eTimes.minInterval
					if readyToEmit(newQI, quorumIndexers[self], *e, busyRate, validators.Len(), eTimes, minMetric, newEventReceived[self]) {

						tooFewHeadsCtr = tooFewHeadsCtr + tooFewHeads
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
							eventPropagation[receiveTime][self][receiveNode] = append(eventPropagation[receiveTime][self][receiveNode], e)
						}

						eventCreationCounter[self] = 0 //reset event creation interval counter
						eventsComplete++               // increment count of events created
						selfParent[self] = *e
						newEventReceived[self] = *validators.NewCounter()

					} else {
						updateEmission[self] = false //require waiting for a new event to be received in the DAG before attempting to create another event
					}
				}
			}
		}
	}

	// print some useful output
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

func processEvent(input abft.EventStore, lchs *abft.TestLachesis, e *QITestEvent, quorumIndexer ancestor.QuorumIndexer, newQI bool, heads *dag.Events, self idx.ValidatorID) (frame idx.Frame) {
	input.SetEvent(e)

	lchs.DagIndexer.Add(e)
	// if err != nil {
	// 	fmt.Println("DagInxeder Add error: ", err)
	// }
	lchs.Lachesis.Build(e)
	lchs.Lachesis.Process(e)
	// if err != nil {
	// 	fmt.Println("Lachesis Process error: ", err)
	// }

	lchs.DagIndexer.Flush()
	// if !newQI {
	// Old  HighestBefore based quorum indexer needs to process the event
	if self == e.Creator() {
		quorumIndexer.ProcessEvent(&e.BaseEvent, true)
	} else {
		quorumIndexer.ProcessEvent(&e.BaseEvent, false)
	}
	// }
	updateHeads(e, heads)
	return lchs.DagIndexer.GetEvent(e.ID()).Frame()
}

func sampleDist(rng *rand.Rand, cumDist []float64) (sample int) {
	// generates a random sample from the distribution used to calculate cumDist (using inverse transform sampling)
	random := rng.Float64()
	for sample = 0; cumDist[sample] <= random && sample < len(cumDist); sample++ {
	}
	if sample <= 0 {
		sample = 1
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
	npts := float64(len(delayData))
	cumDist[0] = float64(binVals[0]) / npts
	for i := 1; i < len(cumDist); i++ {
		cumDist[i] = cumDist[i-1] + binVals[i]/npts
	}
	return cumDist
}

func readyToEmit(newQI bool, quorumIndexer ancestor.QuorumIndexer, e QITestEvent, busyRate *rate.Gauge, nodeCount idx.Validator, times emissionTimes, minMetric ancestor.Metric, newEventReceived pos.WeightCounter) (ready bool) {
	passedTime := times.nowTime - times.prevTime
	if passedTime >= 1500 {
		// maximum limit of event emission interval reached, so emit a new event
		return true
	}

	if len(e.Parents()) > 0 { // not a leaf event (leaf events don't need readiness checking)
		// if newQI {

		// metricRP := quorumIndexer.GetTimingMetric(e.Parents())

		// if metricRP >= ancestor.Metric(minMetric) {
		// 	// fmt.Print(" Timing metric: ", metricRP, " MinMetric: ", minMetric)
		// 	return true
		// }

		// return false // dont emit an event yet
		// } else {
		// 	//below implements a commonly used portion of go-opera event timing decision making

		parents := e.Parents()
		parents = append(parents, quorumIndexer.SelfParentEvent)
		metric := eventMetric(quorumIndexer.GetMetricOfViaParents(parents), e.Seq())
		metric = overheadAdjustedEventMetricF(nodeCount, uint64(busyRate.Rate1()*piecefunc.DecimalUnit), metric) // +++how does busyRate work?
		adjustedPassedTime := passedTime * int(metric) / DecimalUnit
		if adjustedPassedTime < times.minInterval {
			return false
		}
		// }
	}

	return true
}

// Below is copied from go-opera to avoid dependecy on go-opera, and to avoid altering the go-opera implementations of functions so that they are available outside their package

func scalarUpdMetric(diff idx.Event, weight pos.Weight, totalWeight pos.Weight) ancestor.Metric {
	scalarUpdMetricF := piecefunc.NewFunc([]piecefunc.Dot{
		{
			X: 0,
			Y: 0,
		},
		{ // first observed event gives a major metric diff
			X: 1.0 * piecefunc.DecimalUnit,
			Y: 0.66 * piecefunc.DecimalUnit,
		},
		{ // second observed event gives a minor diff
			X: 2.0 * piecefunc.DecimalUnit,
			Y: 0.8 * piecefunc.DecimalUnit,
		},
		{ // other observed event give only a subtle diff
			X: 8.0 * piecefunc.DecimalUnit,
			Y: 0.99 * piecefunc.DecimalUnit,
		},
		{
			X: 100.0 * piecefunc.DecimalUnit,
			Y: 0.999 * piecefunc.DecimalUnit,
		},
		{
			X: 10000.0 * piecefunc.DecimalUnit,
			Y: 0.9999 * piecefunc.DecimalUnit,
		},
	})
	return ancestor.Metric(scalarUpdMetricF(uint64(diff)*piecefunc.DecimalUnit)) * ancestor.Metric(weight) / ancestor.Metric(totalWeight)
}

func updMetric(median, cur, upd idx.Event, validatorIdx idx.Validator, validators *pos.Validators) ancestor.Metric {
	if upd <= median || upd <= cur {
		return 0
	}
	weight := validators.GetWeightByIdx(validatorIdx)
	if median < cur {
		return scalarUpdMetric(upd-median, weight, validators.TotalWeight()) - scalarUpdMetric(cur-median, weight, validators.TotalWeight())
	}
	return scalarUpdMetric(upd-median, weight, validators.TotalWeight())
}

func overheadAdjustedEventMetricF(validatorsNum idx.Validator, busyRate uint64, eventMetric ancestor.Metric) ancestor.Metric {
	return ancestor.Metric(piecefunc.DecimalUnit-overheadF(validatorsNum, busyRate)) * eventMetric / piecefunc.DecimalUnit
}

func overheadF(validatorsNum idx.Validator, busyRate uint64) uint64 {
	validatorsToOverheadF := piecefunc.NewFunc([]piecefunc.Dot{
		{
			X: 0,
			Y: 0,
		},
		{
			X: 25,
			Y: 0.05 * piecefunc.DecimalUnit,
		},
		{
			X: 50,
			Y: 0.2 * piecefunc.DecimalUnit,
		},
		{
			X: 100,
			Y: 0.7 * piecefunc.DecimalUnit,
		},
		{
			X: 200,
			Y: 0.9 * piecefunc.DecimalUnit,
		},
		{
			X: 1000,
			Y: 1.0 * piecefunc.DecimalUnit,
		},
	})
	if busyRate > piecefunc.DecimalUnit {
		busyRate = piecefunc.DecimalUnit
	}
	return validatorsToOverheadF(uint64(validatorsNum)) * busyRate / piecefunc.DecimalUnit
}

func eventMetric(orig ancestor.Metric, seq idx.Event) ancestor.Metric {
	// eventMetricF is a piecewise function for event metric adjustment depending on a non-adjusted event metric
	eventMetricF := piecefunc.NewFunc([]piecefunc.Dot{
		{ // event metric is never zero
			X: 0,
			Y: 0.005 * piecefunc.DecimalUnit,
		},
		{
			X: 0.01 * piecefunc.DecimalUnit,
			Y: 0.03 * piecefunc.DecimalUnit,
		},
		{ // if metric is below ~0.2, then validator shouldn't emit event unless waited very long
			X: 0.2 * piecefunc.DecimalUnit,
			Y: 0.05 * piecefunc.DecimalUnit,
		},
		{
			X: 0.3 * piecefunc.DecimalUnit,
			Y: 0.22 * piecefunc.DecimalUnit,
		},
		{ // ~0.3-0.5 is an optimal metric to emit an event
			X: 0.4 * piecefunc.DecimalUnit,
			Y: 0.45 * piecefunc.DecimalUnit,
		},
		{
			X: 1.0 * piecefunc.DecimalUnit,
			Y: 1.0 * piecefunc.DecimalUnit,
		},
	})
	metric := ancestor.Metric(eventMetricF(uint64(orig)))
	// kick start metric in a beginning of epoch, when there's nothing to observe yet
	if seq <= 2 && metric < 0.9*piecefunc.DecimalUnit {
		metric += 0.1 * piecefunc.DecimalUnit
	}
	if seq <= 1 && metric <= 0.8*piecefunc.DecimalUnit {
		metric += 0.2 * piecefunc.DecimalUnit
	}
	return metric
}

func NewFunc(dots []Dot) func(x uint64) uint64 {
	if len(dots) < 2 {
		panic("too few dots")
	}

	var prevX uint64
	for i, dot := range dots {
		if i >= 1 && dot.X <= prevX {
			panic("non monotonic X")
		}
		if dot.Y > maxVal {
			panic("too large Y")
		}
		if dot.X > maxVal {
			panic("too large X")
		}
		prevX = dot.X
	}

	return Func{
		dots: dots,
	}.Get
}

// Dot is a pair of numbers
type Dot struct {
	X uint64
	Y uint64
}

type Func struct {
	dots []Dot
}

// Mul is multiplication of ratios with integer numbers
func Mul(a, b uint64) uint64 {
	return a * b / DecimalUnit
}

// Div is division of ratios with integer numbers
func Div(a, b uint64) uint64 {
	return a * DecimalUnit / b
}

// Get calculates f(x), where f is a piecewise linear function defined by the pieces
func (f Func) Get(x uint64) uint64 {
	if x < f.dots[0].X {
		return f.dots[0].Y
	}
	if x > f.dots[len(f.dots)-1].X {
		return f.dots[len(f.dots)-1].Y
	}
	// find a piece
	p0 := len(f.dots) - 2
	for i, piece := range f.dots {
		if i >= 1 && i < len(f.dots)-1 && piece.X > x {
			p0 = i - 1
			break
		}
	}
	// linearly interpolate
	p1 := p0 + 1

	x0, x1 := f.dots[p0].X, f.dots[p1].X
	y0, y1 := f.dots[p0].Y, f.dots[p1].Y

	ratio := Div(x-x0, x1-x0)

	return Mul(y0, DecimalUnit-ratio) + Mul(y1, ratio)
}
