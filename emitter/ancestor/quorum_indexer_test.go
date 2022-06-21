package ancestor

// import (
// 	"crypto/sha256"
// 	"fmt"
// 	"testing"
// 	"time"

// 	"github.com/Fantom-foundation/lachesis-base/abft"
// 	"github.com/Fantom-foundation/lachesis-base/hash"
// 	"github.com/Fantom-foundation/lachesis-base/inter/dag"
// 	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
// 	"github.com/Fantom-foundation/lachesis-base/inter/idx"
// 	"github.com/Fantom-foundation/lachesis-base/inter/pos"
// )

// func TestQI(t *testing.T) {
// 	testQI(t, []pos.Weight{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
// }

// func testQI(t *testing.T, weights []pos.Weight) {
// 	eventCount := 5
// 	parentCount := 10

// 	start := time.Now()
// 	testQuorumIndexer(t, weights, eventCount, parentCount, true)
// 	elapsed := time.Since(start)
// 	fmt.Println("NEW took ", elapsed)

// 	start = time.Now()
// 	testQuorumIndexer(t, weights, eventCount, parentCount, false)
// 	elapsed = time.Since(start)
// 	fmt.Println("OLD took ", elapsed)
// }

// func testQuorumIndexer(t *testing.T, weights []pos.Weight, eventCount int, parentCount int, newQI bool) {
// 	// assertar := assert.New(t)

// 	var maxFrame idx.Frame = 0
// 	nodes := tdag.GenNodes(len(weights))

// 	validators := pos.ArrayToValidators(nodes, weights)

// 	diffMetricFn := func(median, current, update idx.Event, validatorIdx idx.Validator) Metric {
// 		return 0
// 	}

// 	// var store *abft.Store
// 	var input *abft.EventStore
// 	var lch *abft.TestLachesis
// 	inputs := make([]abft.EventStore, len(nodes))
// 	lchs := make([]abft.TestLachesis, len(nodes))
// 	quorumIndexers := make([]QuorumIndexer, len(nodes))
// 	for i := 0; i < len(nodes); i++ {
// 		lch, _, input = abft.FakeLachesis(nodes, weights)
// 		lchs[i] = *lch
// 		inputs[i] = *input
// 		// store = lch.IndexedLachesis.Lachesis.Store
// 		quorumIndexers[i] = *NewQuorumIndexer(validators, lch.DagIndex, diffMetricFn, lch)
// 	}

// 	// ordered := map[idx.Epoch]dag.Events{}
// 	if parentCount > len(nodes) {
// 		parentCount = len(nodes)
// 	}

// 	var epoch idx.Epoch = 1

// 	nodeCount := len(nodes)
// 	events := make(map[idx.ValidatorID]dag.Events, nodeCount)

// 	// make events
// 	for i := 0; i < nodeCount*eventCount; i++ {
// 		self := i % nodeCount
// 		creator := nodes[self]
// 		// make
// 		e := &tdag.TestEvent{}
// 		e.SetCreator(creator)
// 		e.SetParents(hash.Events{}) // first parent is empty hash
// 		var parent dag.Event
// 		if ee := events[creator]; len(ee) > 0 {
// 			parent = ee[len(ee)-1] // first parent is creator's previous event, if it exists
// 		}
// 		if parent == nil { // leaf event
// 			e.SetSeq(1)
// 			e.SetLamport(1)
// 		} else { // normal event
// 			e.SetSeq(parent.Seq() + 1)
// 			e.AddParent(parent.ID()) // add previous self event as parent
// 			e.SetLamport(parent.Lamport() + 1)

// 			// create a list of heads to choose parents from
// 			var heads dag.Events
// 			for node, _ := range events {
// 				if node != creator { // already have self parent so exclude it here
// 					if ee := events[node]; len(ee) > 0 {
// 						heads = append(heads, ee[len(ee)-1])
// 					}
// 				}
// 			}
// 			// choose parents using quorumIndexer
// 			quorumIndexers[self].SelfParentEvent = parent.ID()
// 			var parents dag.Events
// 			for j := 0; j < parentCount-1; j++ {

// 				var best int
// 				if newQI {
// 					best = quorumIndexers[self].Choose(parents.IDs(), heads.IDs()) //new quorumIndexer
// 				} else {
// 					best = quorumIndexers[self].SearchStrategy().Choose(parents.IDs(), heads.IDs()) //old quorumIndexer
// 				}
// 				parents = append(parents, heads[best])
// 				// remove chosen head from options
// 				heads[best] = heads[len(heads)-1]
// 				heads = heads[:len(heads)-1]
// 				if len(heads) <= 0 {
// 					break
// 				}
// 			}
// 			// add parents to new event

// 			for _, parent = range parents {
// 				e.AddParent(parent.ID())
// 				if e.Lamport() <= parent.Lamport() {
// 					e.SetLamport(parent.Lamport() + 1)
// 				}
// 			}
// 		}
// 		e.Name = fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator]))
// 		// save and name event
// 		hasher := sha256.New()
// 		hasher.Write(e.Bytes())
// 		var id [24]byte
// 		copy(id[:], hasher.Sum(nil)[:24])
// 		e.SetID(id)
// 		hash.SetEventName(e.ID(), fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator])))
// 		events[creator] = append(events[creator], e)

// 		e.SetEpoch(epoch)

// 		for j := 0; j < len(nodes); j++ {
// 			inputs[j].SetEvent(e)
// 			lchs[j].dagIndexer.Add(e)

// 			lchs[j].Lachesis.Build(e)
// 			lchs[j].Lachesis.Process(e)

// 			lchs[j].dagIndexer.Flush()

// 			quorumIndexers[j].ProcessEvent(&e.BaseEvent, self == i)

// 		}
// 		// fmt.Println("Event: ", i, " ", e.ID(), " Frame: ", lchs[0].dagIndexer.GetEvent(e.ID()).Frame())
// 		if lchs[0].dagIndexer.GetEvent(e.ID()).Frame() > maxFrame {
// 			maxFrame = lchs[0].dagIndexer.GetEvent(e.ID()).Frame()
// 		}
// 	}

// 	if newQI {
// 		fmt.Println("Test using NEW quorumIndexer")
// 	} else {
// 		fmt.Println("Test using OLD quorumIndexer")
// 	}
// 	fmt.Println("Max Frame: ", maxFrame)
// 	fmt.Println("Number of nodes: ", nodeCount)
// 	fmt.Println("Number of Events: ", nodeCount*eventCount)
// 	fmt.Println("Max Parents:", parentCount)
// }

// func testQuorumIndexerNewQIOnly(t *testing.T, weights []pos.Weight, eventCount int) {
// 	// assertar := assert.New(t)
// 	var maxFrame idx.Frame = 0
// 	nodes := tdag.GenNodes(len(weights))

// 	lch, _, input := abft.FakeLachesis(nodes, weights)

// 	validators := pos.ArrayToValidators(nodes, weights)

// 	diffMetricFn := func(median, current, update idx.Event, validatorIdx idx.Validator) Metric {
// 		return 0
// 	}

// 	store := lch.IndexedLachesis.Lachesis.Store
// 	quorumIndexer := NewQuorumIndexer(validators, lch.DagIndex, diffMetricFn, store)

// 	parentCount := 2
// 	if parentCount > len(nodes) {
// 		parentCount = len(nodes)
// 	}

// 	var epoch idx.Epoch = 1

// 	nodeCount := len(nodes)
// 	events := make(map[idx.ValidatorID]dag.Events, nodeCount)

// 	// make events
// 	for i := 0; i < nodeCount*eventCount; i++ {
// 		self := i % nodeCount
// 		creator := nodes[self]
// 		// make
// 		e := &tdag.TestEvent{}
// 		e.SetCreator(creator)
// 		e.SetParents(hash.Events{}) // first parent is empty hash
// 		var parent dag.Event
// 		if ee := events[creator]; len(ee) > 0 {
// 			parent = ee[len(ee)-1] // first parent is creator's previous event, if it exists
// 		}
// 		if parent == nil { // leaf event
// 			e.SetSeq(1)
// 			e.SetLamport(1)
// 		} else { // normal event
// 			e.SetSeq(parent.Seq() + 1)
// 			e.AddParent(parent.ID()) // add previous self event as parent
// 			e.SetLamport(parent.Lamport() + 1)

// 			// create a list of heads to choose parents from
// 			var heads dag.Events
// 			for node, _ := range events {
// 				if node != creator { // already have self parent so exclude it here
// 					if ee := events[node]; len(ee) > 0 {
// 						heads = append(heads, ee[len(ee)-1])
// 					}
// 				}
// 			}
// 			// choose parents using quorumIndexer
// 			quorumIndexer.SelfParentEvent = parent.ID()
// 			var parents dag.Events
// 			for j := 0; j < parentCount; j++ {

// 				best := quorumIndexer.Choose(parents.IDs(), heads.IDs()) //new quorumIndexer
// 				parents = append(parents, heads[best])
// 				// remove chosen head from options
// 				heads[best] = heads[len(heads)-1]
// 				heads = heads[:len(heads)-1]
// 				if len(heads) <= 0 {
// 					break
// 				}
// 			}
// 			// add parents to new event

// 			for _, parent = range parents {
// 				e.AddParent(parent.ID())
// 				if e.Lamport() <= parent.Lamport() {
// 					e.SetLamport(parent.Lamport() + 1)
// 				}
// 			}
// 		}
// 		e.Name = fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator]))
// 		// save and name event
// 		hasher := sha256.New()
// 		hasher.Write(e.Bytes())
// 		var id [24]byte
// 		copy(id[:], hasher.Sum(nil)[:24])
// 		e.SetID(id)
// 		hash.SetEventName(e.ID(), fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator])))
// 		events[creator] = append(events[creator], e)

// 		e.SetEpoch(epoch)
// 		// e.SetID(lch.uniqueDirtyID.sample())
// 		input.SetEvent(e)
// 		lch.dagIndexer.Add(e)

// 		lch.Lachesis.Build(e)
// 		lch.Lachesis.Process(e)

// 		lch.dagIndexer.Flush()

// 		if lch.dagIndexer.GetEvent(e.ID()).Frame() > maxFrame {
// 			maxFrame = lch.dagIndexer.GetEvent(e.ID()).Frame()
// 		}

// 	}
// 	fmt.Println("Test using NEW quorumIndexer")
// 	fmt.Println("Max Frame: ", maxFrame)
// 	fmt.Println("Number of nodes: ", nodeCount)
// 	fmt.Println("Number of Events: ", nodeCount*eventCount)
// }
