package quorumIndexer

import (
	"crypto/sha256"
	"fmt"
	"math"
	"math/rand"
	"sync"
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
	DecimalUnit         = 1e6 // DecimalUnit is used to shift the decimal place in order to represent fractional values using integers
	maxVal              = math.MaxUint64/uint64(DecimalUnit) - 1
	maxEmissionInterval = 2000 // maximum time interval between event emission
)

type emissionTimes struct {
	nowTime      int
	prevTime     int
	minInterval  int
	maxInterval  int
	releaseTime  int
	delayRelease bool
}

type QITestEvents []*QITestEvent

type QITestEvent struct {
	tdag.TestEvent
	creationTime int
}

func TestQI(t *testing.T) {
	// numNodes := 70
	// nodes := []int{20, 30, 40, 50, 60, 70, 80, 90, 100}
	nodes := []int{20}
	// stakeDist := stakeCumDist()             // for stakes drawn from distribution
	// stakeRNG := rand.New(rand.NewSource(0)) // for stakes drawn from distribution
	for _, numNodes := range nodes {

		weights := make([]pos.Weight, numNodes)
		for i, _ := range weights {
			//for equal stake
			weights[i] = pos.Weight(1)

			// for non-equal stake
			// weights[i] = pos.Weight(sampleDist(stakeRNG, stakeDist)) // take a random sample from stake data distribution
		}
		testQI(t, weights)
	}
}

func testQI(t *testing.T, weights []pos.Weight) {

	// parentCount := []int{3, 4, 5, 6, 7, 8, 9, 10}
	QIParentCount := []int{3}
	randParentCount := []int{0}

	meanDelay := 200
	stdDelay := 50
	maxDelay := meanDelay + 3*stdDelay + 1
	eventInterval := make([]int, len(weights))
	for i := range eventInterval {
		eventInterval[i] = 10 //8000 // this determines how many milliseconds each node waits after creating an event to be allowed to create its next event
	}
	var metricParameter float64
	metricParameter = 100 // a general purpose parameter for use in testign/development
	// offlineNodes := false // all nodes create events
	offlineNodes := true // only Quorum nodes create events
	for metricParameter > 0.0 {
		fmt.Println("")
		fmt.Println("Metric Parameter: ", metricParameter)
		for i := range QIParentCount {
			offlineNodes = true
			start := time.Now()
			// testQuorumIndexerLatency(t, weights, QIParentCount[i], randParentCount[i], true, maxDelay, meanDelay, stdDelay, eventInterval, metricParameter, offlineNodes)
			elapsed := time.Since(start)
			fmt.Println("Root progress parent selection, quorum online. Took ", elapsed)

			offlineNodes = false
			start = time.Now()
			testQuorumIndexerLatency(t, weights, QIParentCount[i], randParentCount[i], true, maxDelay, meanDelay, stdDelay, eventInterval, metricParameter, offlineNodes)
			elapsed = time.Since(start)
			fmt.Println("Root progress parent selection, all online. Took ", elapsed)

			// offlineNodes = true
			// start = time.Now()
			// testQuorumIndexerLatency(t, weights, eventCount, QIParentCount[i], randParentCount[i], false, maxDelay, meanDelay, stdDelay, eventInterval, metricParameter, offlineNodes)
			// elapsed = time.Since(start)
			// fmt.Println("HighestBefore parent selection, quourm online. Took ", elapsed)

			// offlineNodes = false
			// start = time.Now()
			// testQuorumIndexerLatency(t, weights, eventCount, QIParentCount[i], randParentCount[i], false, maxDelay, meanDelay, stdDelay, eventInterval, metricParameter, offlineNodes)
			// elapsed = time.Since(start)
			// fmt.Println("HighestBefore parent selection, all online. Took ", elapsed)
		}
		metricParameter = metricParameter + 100
	}
}

var mutex sync.Mutex

func testQuorumIndexerLatency(t *testing.T, weights []pos.Weight, QIParentCount int, randParentCount int, newQI bool, maxDelay int, meanDelay int, stdDelay int, eventInterval []int, metricParameter float64, offlineNodes bool) {
	randSrc := rand.New(rand.NewSource(0))       // use a fixed seed of 0 for comparison between runs
	randParentRNG := rand.New(rand.NewSource(0)) // use a fixed seed of 0 for comparison between runs
	delayRNG := rand.New(rand.NewSource(0))      // use a fixed seed of 0 for comparison between runs
	randEvRNG := rand.New(rand.NewSource(0))     // use a fixed seed of 0 for comparison between runs
	randEvRate := 0.00                           // sets the probability that an event will be created randomly
	delayDist := delayCumDist()
	maxDelay = len(delayDist) + 10

	nodeCount := len(weights)

	eTimes := make([]emissionTimes, nodeCount)

	// create a 2D slice with coordinates [time][node] that is used to store delayed transmission of events between nodes
	//each time coordinate corresponds to 1 millisecond of delay between a pair of nodes
	eventPropagation := make([][][]*QITestEvent, maxDelay)
	for t := range eventPropagation {
		eventPropagation[t] = make([][]*QITestEvent, nodeCount)
		for node := range eventPropagation[t] {
			eventPropagation[t][node] = make([]*QITestEvent, 0)
		}
	}

	// create a 2D slice with coordinates [node][node] that is used to store the delays/latencies in milliseconds between pairs of nodes
	delays := make([][]int, nodeCount)
	for i := range delays {
		delays[i] = make([]int, nodeCount)
	}

	//initial delay to avoid synchronous events
	initialDelay := make([]int, nodeCount)

	for i := range initialDelay {
		initialDelay[i] = randSrc.Intn(maxDelay)
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

	//Set some nodes as offline for testing
	sortWeights := validators.SortedWeights()
	sortedIDs := validators.SortedIDs()
	onlineStake := validators.TotalWeight()
	online := make(map[idx.ValidatorID]bool)
	for i := len(sortWeights) - 1; i >= 0; i-- {
		online[sortedIDs[i]] = true
		if offlineNodes {
			if onlineStake-sortWeights[i] >= validators.Quorum() {
				onlineStake -= sortWeights[i]
				online[sortedIDs[i]] = false
			}
		}
	}

	// setup event creation values
	eventCreationCounter := make([]int, nodeCount)
	eventCreationInterval := make([]int, nodeCount)
	longestInterval := 0

	for i := range eventCreationCounter {
		// eventCreationCounter[i] = randSrc.Intn(maxDelay)
		// standard minimum interval between creating events
		eventCreationInterval[i] = eventInterval[i]
		// minimum interval between creating events so that large stake nodes have smaller interval and create more events

		// eventCreationInterval[i] = int(float64(eventInterval[i]) * float64(sortWeights[0]) / float64(weights[i]))

		if eventCreationInterval[i] > longestInterval {
			longestInterval = eventCreationInterval[i] // longest interval is needed later for checking if the network stalls (no events are produced)
		}
	}
	tooFewHeadsCtr := 0
	tooFewHeads := 0

	var epoch idx.Epoch = 1
	var maxFrame idx.Frame = 0

	bufferedEvents := make([]QITestEvents, nodeCount)
	updatedDAG := make([]bool, nodeCount)

	rand.Seed(0) // re-seed for fixed permutations between runs
	eventsComplete := make([]int, nodeCount)
	isLeaf := make([]bool, nodeCount)
	for node := range isLeaf {
		isLeaf[node] = true
		updatedDAG[node] = true
	}

	// selfParent := make([]tdag.TestEvent, nodeCount)
	selfParent := make([]QITestEvent, nodeCount)
	newEventReceived := make([]pos.WeightCounter, nodeCount)
	for i, _ := range newEventReceived {
		newEventReceived[i] = *validators.NewCounter()
	}

	wg := sync.WaitGroup{}

	timeIdx := maxDelay - 1
	time := -1

	noNewEvents := 0

	tMax := 50000 // in units of milliseconds; 60 000 = simulation of 1 minute of network activity
	// now start the simulation
	for time < tMax {
		// move forward one timestep
		timeIdx = (timeIdx + 1) % maxDelay
		time++
		noNewEvents++
		if time%1000 == 0 {
			// fmt.Println("")
			fmt.Print(" TIME: ", time)
			// fmt.Println("")
		}
		if noNewEvents > maxDelay+longestInterval+10 {
			// fmt.Println("")
			// fmt.Println("EVENT CREATION STALLED")
			// fmt.Println("")
			// break
		}
		// Check to see if new events are received by nodes
		// if they are, do the appropriate updates for the received event
		for receiveNode := 0; receiveNode < nodeCount; receiveNode++ {
			wg.Add(1)
			go func(receiveNode int) {
				defer wg.Done()
				// check for events to be received by other nodes (including self)
				{
					for i := 0; i < len(eventPropagation[timeIdx][receiveNode]); i++ {
						e := eventPropagation[timeIdx][receiveNode][i]
						updatedDAG[receiveNode] = true // with a new received event, DAG progress may have improved enough to allow event emission, set this flag to check

						//add new event to buffer for cheecking if events are ready to put in DAG
						bufferedEvents[receiveNode] = append(bufferedEvents[receiveNode], e)
						noNewEvents = 0
					}
					//clear the events at this time index
					mutex.Lock()
					eventPropagation[timeIdx][receiveNode] = eventPropagation[timeIdx][receiveNode][:0]
					mutex.Unlock()
				}
				// it is required that all of an event's parents have been received before adding to DAG
				// loop through buffer to check for events that can be processed
				process := make([]bool, len(bufferedEvents[receiveNode]))
				for i, buffEvent := range bufferedEvents[receiveNode] {
					process[i] = true
					//check if all parents are in the DAG
					for _, parent := range buffEvent.Parents() {
						if lchs[receiveNode].DagIndexer.GetEvent(parent) == nil {
							// a parent is not yet in the DAG, so don't process this event yet
							process[i] = false
							break
						}
					}
					if process[i] {
						// buffered event has all parents in the DAG and can now be processed
						frame := processEvent(inputs[receiveNode], &lchs[receiveNode], buffEvent, &quorumIndexers[receiveNode], newQI, &headsAll[receiveNode], nodes[receiveNode], time)
						if frame > maxFrame {
							// quorumIndexers[receiveNode].PrintSubgraphK(buffEvent.Frame()-1, buffEvent.ID())
							maxFrame = frame
						}

						// keep a count of nodes that have created events after self's most recent event was created
						if buffEvent.creationTime > selfParent[receiveNode].creationTime {
							newEventReceived[receiveNode].Count(buffEvent.Creator())
						}
					}
				}

				//remove processed events from buffer
				temp := make([]*QITestEvent, len(bufferedEvents[receiveNode]))
				copy(temp, bufferedEvents[receiveNode])
				bufferedEvents[receiveNode] = bufferedEvents[receiveNode][:0] //clear buffer
				for i, processed := range process {
					if processed == false {
						bufferedEvents[receiveNode] = append(bufferedEvents[receiveNode], temp[i]) // put unprocessed event back in the buffer
					}

				}
			}(receiveNode)
		}
		wg.Wait()
		// Build events and check timing condition
		for self := 0; self < nodeCount; self++ {
			wg.Add(1)
			go func(self int) {
				defer wg.Done()
				eventCreationCounter[self]++ // count time since last event created
				if initialDelay[self] > 0 {
					// an initial delay in creating the first event at the start of the simulation
					initialDelay[self]--
				} else {
					//self node is ready to try creating a new event

					//create the event datastructure
					selfID := nodes[self]
					e := &QITestEvent{}
					e.SetCreator(selfID)
					e.SetParents(hash.Events{}) // first parent is empty hash

					var parents dag.Events
					if isLeaf[self] { // leaf event
						e.SetSeq(1)
						e.SetLamport(1)
					} else { // normal event
						e.SetSeq(selfParent[self].Seq() + 1)
						e.SetLamport(selfParent[self].Lamport() + 1)
						parents = append(parents, &selfParent[self].BaseEvent) // always use self's previous event as a parent
					}

					// get heads for parent selection
					var heads dag.Events
					for _, head := range headsAll[self] {
						heads = append(heads, head)
					}
					for i := range heads {
						if selfParent[self].BaseEvent.ID() == heads[i].ID() {
							// remove the self parent from options, it is already a parent
							heads[i] = heads[len(heads)-1]
							heads = heads[:len(heads)-1]
							break
						}
					}

					quorumIndexers[self].SelfParentEvent = selfParent[self].ID() // quorumIndexer needs to know the self's previous event

					if len(heads) < QIParentCount+randParentCount-1 {
						tooFewHeads = 1 // any parent selection method will give the same result (all available heads), count occurances
					} else {
						tooFewHeads = 0
					}

					if len(heads) >= 1 { // need at least one head to select parents
						//iteratively select the best parent from the list of heads using QI
						for j := 0; j < QIParentCount-1; j++ {
							var best int
							if isLeaf[self] {
								// fmt.Print("self ", self, " len ", len(heads))
								best = randSrc.Intn(len(heads))
							} else {
								if newQI {
									best = quorumIndexers[self].Choose(parents.IDs(), heads.IDs()) //new quorumIndexer
								} else {
									best = quorumIndexers[self].SearchStrategy().Choose(parents.IDs(), heads.IDs()) //old quorumIndexer
								}
							}
							parents = append(parents, heads[best])
							// remove chosen parent from head options
							heads[best] = heads[len(heads)-1]
							heads = heads[:len(heads)-1]
							if len(heads) <= 0 {
								break
							}
						}
						//now select random parents +++TODO make this before or after QI selection?
						for j := 0; j < randParentCount-1; j++ {
							if len(heads) <= 0 {
								break
							}
							randParent := randParentRNG.Intn(len(heads))
							parents = append(parents, heads[randParent])
							// remove chosen parent from head options
							heads[randParent] = heads[len(heads)-1]
							heads = heads[:len(heads)-1]
						}
					}

					// parent selection is complete, add selected parents to new event
					for _, parent := range parents {
						e.AddParent(parent.ID())
						if e.Lamport() <= parent.Lamport() {
							e.SetLamport(parent.Lamport() + 1)
						}
					}

					// name and ID the event
					e.SetEpoch(epoch)
					e.Name = fmt.Sprintf("%03d%04d", self, e.Seq())
					hasher := sha256.New()
					hasher.Write(e.Bytes())
					var id [24]byte
					copy(id[:], hasher.Sum(nil)[:24])
					e.SetID(id)
					hash.SetEventName(e.ID(), fmt.Sprintf("%03d%04d", self, e.Seq()))

					// setup some timing information for event timing
					e.creationTime = time
					eTimes[self].nowTime = time
					eTimes[self].prevTime = selfParent[self].creationTime
					eTimes[self].minInterval = eventCreationInterval[self]
					eTimes[self].maxInterval = 2 * eTimes[self].minInterval

					createRandEvent := randEvRNG.Float64() < randEvRate // used for introducing randomly created events
					if online[selfID] == true {
						// self is online
						if createRandEvent || isLeaf[self] || readyToEmit(newQI, quorumIndexers[self], *e, busyRate, &eTimes[self], metricParameter, newEventReceived[self], QIParentCount+randParentCount, idx.Validator(nodeCount), online) {
							//create an event if (i) leaf event, or (ii) event timing condition is met, or (iii) a random event is created
							isLeaf[self] = false                          // only create one leaf event
							noNewEvents = 0                               //reset timer counting time interval during which no event has been created
							tooFewHeadsCtr = tooFewHeadsCtr + tooFewHeads // count events where there were not enough available heads for parent selection

							//now start propagation of event to other nodes
							delay := 1
							for receiveNode := 0; receiveNode < nodeCount; receiveNode++ {
								if receiveNode != self {
									delay = sampleDist(delayRNG, delayDist) // get a random delay from data distribution
								} else {
									delay = 1 // no delay to send to self (self will 'recieve' its own event after time increment at the top of the main loop)
								}
								receiveTime := (timeIdx + delay) % maxDelay // time index for the circular buffer
								mutex.Lock()
								eventPropagation[receiveTime][receiveNode] = append(eventPropagation[receiveTime][receiveNode], e) // add the event to the buffer
								mutex.Unlock()
							}

							eventCreationCounter[self] = 0                    //reset event creation interval counter for this node
							eventsComplete[self]++                            // increment count of events created for this node
							selfParent[self] = *e                             //update self parent to be this new event
							newEventReceived[self] = *validators.NewCounter() // reset count of new events recieved from other node since self's most recent event
						}
					}
				}
			}(self)
		}
		wg.Wait()
	}
	busyRate.Stop()
	// print some useful output
	fmt.Println("")
	if newQI {
		fmt.Println("Test using NEW quorumIndexer")
	} else {
		fmt.Println("Test using OLD quorumIndexer")
	}

	// print number of events created by each node
	var totalEventsComplete int = 0
	for i, nEv := range eventsComplete {
		totalEventsComplete += nEv
		fmt.Println("Stake: ", weights[i], "events: ", nEv, " events/stake: ", float64(nEv)/float64(weights[i]))
	}
	fmt.Println("Max Frame: ", maxFrame)
	fmt.Println("Time ", float64(time)/1000.0, " seconds")
	fmt.Println("Frames per second: ", (1000.0*float64(maxFrame))/float64(time))
	fmt.Println("Number of Events: ", totalEventsComplete)

	numOnlineNodes := 0
	for _, node := range online {
		if node {
			numOnlineNodes++
		}
	}
	fmt.Println("Event rate per (online) node: ", float64(totalEventsComplete)/float64(numOnlineNodes)/(float64(time)/1000.0))
	fmt.Println("Average frames per event per (online) node: ", (float64(maxFrame))/(float64(totalEventsComplete)/float64(numOnlineNodes)))

	fmt.Println("Number of nodes: ", nodeCount)
	fmt.Println("Number of nodes online: ", numOnlineNodes)
	fmt.Println("Max Total Parents: ", QIParentCount+randParentCount, " Max QI Parents:", QIParentCount, " Max Random Parents", randParentCount)
	fmt.Println("Fraction of events with equal or fewer heads than desired parents: ", float64(tooFewHeadsCtr)/float64(totalEventsComplete))
}

func updateHeads(newEvent dag.Event, heads *dag.Events) {
	// remove newEvent's parents from heads
	for _, parent := range newEvent.Parents() {
		for i := 0; i < len(*heads); i++ {
			if (*heads)[i].ID() == parent {
				(*heads)[i] = (*heads)[len(*heads)-1]
				*heads = (*heads)[:len(*heads)-1]
				break
			}
		}
	}
	*heads = append(*heads, newEvent) //add newEvent to heads
}

func processEvent(input abft.EventStore, lchs *abft.TestLachesis, e *QITestEvent, quorumIndexer *ancestor.QuorumIndexer, newQI bool, heads *dag.Events, self idx.ValidatorID, time int) (frame idx.Frame) {
	input.SetEvent(e)

	lchs.DagIndexer.Add(e)
	lchs.Lachesis.Build(e)
	lchs.Lachesis.Process(e)

	lchs.DagIndexer.Flush()
	// HighestBefore based quorum indexer needs to process the event
	if self == e.Creator() {
		quorumIndexer.ProcessEvent(&e.BaseEvent, true)
	} else {
		quorumIndexer.ProcessEvent(&e.BaseEvent, false)
	}
	updateHeads(e, heads)
	quorumIndexer.UpdateTimingStats(float64(time)-float64(e.creationTime), e.Creator())
	return lchs.DagIndexer.GetEvent(e.ID()).Frame()
}

func sampleDist(rng *rand.Rand, cumDist []float64) (sample int) {
	// generates a random sample from the distribution used to calculate cumDist (using inverse transform sampling)
	random := rng.Float64()
	for sample = 1; cumDist[sample] <= random && sample < len(cumDist); sample++ {
	}
	if sample <= 0 {
		sample = 1 // the distributions used here should not be negative or zero, an explicit check
		fmt.Println("")
		fmt.Println("WARNING: distribution sample was <=0, and reset to 1")
	}
	return sample
}

func stakeCumDist() (cumDist []float64) {
	// the purpose of this function is to caluclate a cumulative distribution of validator stake for use in creating random samples from the data distribution

	//list of validator stakes in July 2022
	stakeData := [...]float64{198081564.62, 170755849.45, 145995219.17, 136839786.82, 69530006.55, 40463200.25, 39124627.82, 32452971, 29814402.94, 29171276.63, 26284696.12, 25121739.54, 24461049.53, 23823498.37, 22093834.4, 21578984.4, 20799555.11, 19333530.31, 18250949.01, 17773018.94, 17606393.73, 16559031.91, 15950172.21, 12009825.67, 11049478.07, 9419996.86, 9164450.96, 9162745.35, 7822093.53, 7540197.22, 7344958.29, 7215437.9, 6922757.07, 6556643.44, 5510793.7, 5228201.11, 5140257.3, 4076474.17, 3570632.17, 3428553.68, 3256601.94, 3185019, 3119162.23, 3011027.22, 2860160.77, 2164550.78, 1938492.01, 1690762.63, 1629428.73, 1471177.28, 1300562.06, 1237812.75, 1199822.32, 1095856.64, 1042099.38, 1020613.06, 1020055.55, 946528.43, 863022.57, 826015.44, 800010, 730537, 623529.61, 542996.04, 538920.36, 536288, 519803.37, 505401, 502231, 500100, 500001, 500000}
	stakeDataInt := make([]int, len(stakeData))
	// find the maximum stake in the data
	maxStake := 0
	for i, stake := range stakeData {
		stakeDataInt[i] = int(stake)
		if int(stake) > maxStake {
			maxStake = int(stake)
		}
	}
	// calculate the distribution of the data by dividing into bins
	binVals := make([]float64, maxStake+1)
	for _, stake := range stakeDataInt {
		binVals[stake]++
	}

	//now calculate the cumulative distribution of the delay data
	cumDist = make([]float64, len(binVals))
	npts := float64(len(stakeDataInt))
	cumDist[0] = float64(binVals[0]) / npts
	for i := 1; i < len(cumDist); i++ {
		cumDist[i] = cumDist[i-1] + binVals[i]/npts
	}
	return cumDist
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

	// calculate the distribution of the delay data by dividing into bins
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

func readyToEmit(newQI bool, quorumIndexer ancestor.QuorumIndexer, e QITestEvent, busyRate *rate.Gauge, times *emissionTimes, metricParameter float64, newEventReceived pos.WeightCounter, nParents int, nodeCount idx.Validator, online map[idx.ValidatorID]bool) (ready bool) {
	passedTime := times.nowTime - times.prevTime
	// if passedTime >= maxEmissionInterval {
	// maximum limit of event emission interval reached, so emit a new event
	// fmt.Println("Timeout: time", times.nowTime, " Validator: ", e.Creator())
	// return true
	// }

	if len(e.Parents()) > 1 { // need at aleast one parent other than self

		// ***Logistic Online Growth (requries a reliable estimate of which nodes are online)****
		// if passedTime > times.minInterval {
		// 	// metric := quorumIndexer.ExponentialTimingConditionByCount(e.Parents(), nParents, newEventReceived.Sum())
		// 	kNew, metric := quorumIndexer.LogisticTimingConditionByCountOnline(e.Parents(), nParents, online)
		// 	// kNew, metric := quorumIndexer.BernoulliTimingConditionByCountOnline(e.Parents(), nParents, online)

		// 	if metric {
		// 		// tk := quorumIndexer.LogisticTimingDeltat(e.Parents(), nParents, newEventReceived.Sum())
		// 		// fmt.Println("k: ", kNew, ", Del t: ", passedTime, ", Now t: ", times.nowTime)
		// 		fmt.Print(",", kNew)
		// 		// fmt.Print(",", float64(passedTime))
		// 		return true
		// 	}
		// }

		// ***Logistic Online Growth and Time****
		if passedTime > times.minInterval {
			// metric := quorumIndexer.ExponentialTimingConditionByCount(e.Parents(), nParents, newEventReceived.Sum())
			kNew, metric := quorumIndexer.LogisticTimingConditionByCountOnlineAndTime(metricParameter, float64(passedTime), e.Parents(), nParents, online)
			if metric {
				// tk := quorumIndexer.LogisticTimingDeltat(e.Parents(), nParents, newEventReceived.Sum())
				// fmt.Println("k: ", kCond, ", Del t: ", passedTime, ", Now t: ", times.nowTime)
				// fmt.Print(",", float64(passedTime))
				fmt.Print(",", kNew)
				return true
			}
		}

		// ***Logistic Growth and Time****
		// if passedTime > times.minInterval {
		// 	// metric := quorumIndexer.ExponentialTimingConditionByCount(e.Parents(), nParents, newEventReceived.Sum())
		// 	kNew, metric := quorumIndexer.LogisticTimingConditionByCountAndTime(float64(passedTime), e.Parents(), nParents, newEventReceived.Sum())
		// 	if metric {
		// 		// tk := quorumIndexer.LogisticTimingDeltat(e.Parents(), nParents, newEventReceived.Sum())
		// 		// fmt.Println("k: ", kCond, ", Del t: ", passedTime, ", Now t: ", times.nowTime)
		// 		// fmt.Print(",", float64(passedTime))
		// 		fmt.Print(",", kNew)
		// 		return true
		// 	}
		// }

		// ***go-opera style logistic event timing***
		// this is very similar to the ***Logistic Growth and Time**** condition above, but written in the format of previous timing functions
		// parents := e.Parents()
		// metric := eventMetricLogistic(quorumIndexer.GetMetricOfLogistic(parents, len(parents), online), e.Seq())

		// // // +++TODO what is the reasoning for the adjustment based on number of nodes and busy rate? A network size adjustment isn't needed
		// // // unless it is required as part of the busy rate adjustment
		// // // busy rate is a measurement of tx rate, but tx are not simulated so the function makes no adjustment in these simulations
		// metric = overheadAdjustedEventMetricF(nodeCount, uint64(busyRate.Rate1()*piecefunc.DecimalUnit), metric)
		// adjustedPassedTime := passedTime * int(float64(metric)) / DecimalUnit
		// // the below condition is written in the form of the existing go-opera timing condition (where metricParameter=110)
		// // however in the context of logistic growth it should be interpreted as metricParameter being a factor that transforms between
		// // units of 'DAG progress' time and real time in milliseconds
		// // in current go-opera metricParameter is fixed at 110; however, this could be changed so that it adjusts to network conditions
		// // by using measurements of latencies obtained from differences in event creation and receive times,
		// // see an implementation in LogisticTimingConditionByCountAndTime via timingMedianMean
		// // adjustedPassedTime is then (aside from a factor of metricParameter unit scaling) the squared geometric mean of the two times
		// if adjustedPassedTime >= int(metricParameter) {
		// 	return true
		// }

		// // ***go-opera event timing***

		// if passedTime > times.minInterval {
		// 	parents := e.Parents()
		// 	metric := eventMetric(quorumIndexer.GetMetricOfViaParents(parents), e.Seq())
		// 	metric = overheadAdjustedEventMetricF(nodeCount, uint64(busyRate.Rate1()*piecefunc.DecimalUnit), metric) // +++how does busyRate work?
		// 	adjustedPassedTime := passedTime * int(float64(metric)) / DecimalUnit
		// 	// fmt.Println(" Timing metric: ", adjustedPassedTime, " Condition: ", times.minInterval)
		// 	// if adjustedPassedTime >= times.minInterval {
		// 	if adjustedPassedTime >= int(metricParameter) {
		// 		// quorumIndexer.LogisticTimingCondition3(e.Parents(), nParents)
		// 		// fmt.Print(",", float64(passedTime))
		// 		kNew, _ := quorumIndexer.LogisticTimingConditionByCountOnlineAndTime(metricParameter, float64(passedTime), e.Parents(), nParents, online)
		// 		fmt.Print(",", kNew)
		// 		return true
		// 	}
		// }

		// ***RECEIVED STAKE***
		// fmt.Println(" Timing metric: ", float64(newEventReceived.Sum()), " Condition: ", (metricParameter)*float64(newEventReceived.Quorum))
		// if passedTime > times.minInterval {
		// 	if float64(newEventReceived.Sum()) > (metricParameter)*float64(newEventReceived.Quorum) {
		// 		return true
		// 	}
		// }

		// // ***Fixed Time interval ***
		// if float64(passedTime) >= metricParameter {
		// 	var maxFrame idx.Frame = 0
		// 	for _, parent := range e.Parents() {
		// 		if quorumIndexer.Dagi.GetEvent(parent).Frame() > maxFrame {
		// 			maxFrame = quorumIndexer.Dagi.GetEvent(parent).Frame()
		// 		}
		// 	}
		// 	metric := quorumIndexer.EventRootKnowledgeQByCount(maxFrame, *e.SelfParent(), e.Parents())
		// 	fmt.Print(", ", metric)
		// 	return true
		// }
	}
	return false
}

func eventMetricLogistic(orig ancestor.Metric, seq idx.Event) ancestor.Metric {
	return orig
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
