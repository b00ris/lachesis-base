package ancestor

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/abft/election"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/utils/wmedian"
)

type sortedKIdx []KIdx

type KIdx struct {
	K    float64
	Root election.RootAndSlot
}

type sortedRootProgressMetrics []RootProgressMetrics

type RootProgressMetrics struct {
	idx int
	// head                  hash.Event
	// CreatorFrame          idx.Frame
	// HeadFrame             idx.Frame
	NewObservedRootWeight pos.WeightCounter
	NewFCWeight           pos.WeightCounter
	NewRootKnowledge      uint64
}

type MetricStats struct {
	expMovAv  float64 // exponential moving average
	expMovVar float64 // exponential moving variance
	expCoeff  float64 // exponential constant
}

type DagIndex interface {
	dagidx.VectorClock
}
type DiffMetricFn func(median, current, update idx.Event, validatorIdx idx.Validator) Metric

type QuorumIndexer struct {
	Dagi       DagIndex
	validators *pos.Validators

	SelfParentEvent     hash.Event
	SelfParentEventFlag bool

	lachesis    *abft.Lachesis
	r           *rand.Rand
	metricStats MetricStats

	CreatorFrame idx.Frame

	globalMatrix     Matrix
	selfParentSeqs   []idx.Event
	globalMedianSeqs []idx.Event
	dirty            bool
	searchStrategy   SearchStrategy

	diffMetricFn DiffMetricFn
}

func NewMetricStats() MetricStats {
	return MetricStats{
		expMovAv:  0,
		expMovVar: 0,
		expCoeff:  0.05,
	}
}

func NewQuorumIndexer(validators *pos.Validators, dagi DagIndex, diffMetricFn DiffMetricFn, lachesis *abft.Lachesis) *QuorumIndexer {
	return &QuorumIndexer{
		globalMatrix:        NewMatrix(validators.Len(), validators.Len()),
		globalMedianSeqs:    make([]idx.Event, validators.Len()),
		selfParentSeqs:      make([]idx.Event, validators.Len()),
		Dagi:                dagi,
		validators:          validators,
		diffMetricFn:        diffMetricFn,
		dirty:               true,
		lachesis:            lachesis,
		r:                   rand.New(rand.NewSource(time.Now().UnixNano())),
		SelfParentEventFlag: false,
		metricStats:         NewMetricStats(),
	}
}

type Matrix struct {
	buffer  []idx.Event
	columns idx.Validator
}

func NewMatrix(rows, cols idx.Validator) Matrix {
	return Matrix{
		buffer:  make([]idx.Event, rows*cols),
		columns: cols,
	}
}

func (m Matrix) Row(i idx.Validator) []idx.Event {
	return m.buffer[i*m.columns : (i+1)*m.columns]
}

func (m Matrix) Clone() Matrix {
	buffer := make([]idx.Event, len(m.buffer))
	copy(buffer, m.buffer)
	return Matrix{
		buffer,
		m.columns,
	}
}

func seqOf(seq dagidx.Seq) idx.Event {
	if seq.IsForkDetected() {
		return math.MaxUint32/2 - 1
	}
	return seq.Seq()
}

type weightedSeq struct {
	seq    idx.Event
	weight pos.Weight
}

func (ws weightedSeq) Weight() pos.Weight {
	return ws.weight
}

func (h *QuorumIndexer) ProcessEvent(event dag.Event, selfEvent bool) {
	vecClock := h.Dagi.GetMergedHighestBefore(event.ID())
	creatorIdx := h.validators.GetIdx(event.Creator())
	// update global matrix
	for validatorIdx := idx.Validator(0); validatorIdx < h.validators.Len(); validatorIdx++ {
		seq := seqOf(vecClock.Get(validatorIdx))
		h.globalMatrix.Row(validatorIdx)[creatorIdx] = seq
		if selfEvent {
			h.selfParentSeqs[validatorIdx] = seq
		}
	}
	h.dirty = true
}

func (h *QuorumIndexer) recacheState() {
	// update median seqs
	for validatorIdx := idx.Validator(0); validatorIdx < h.validators.Len(); validatorIdx++ {
		pairs := make([]wmedian.WeightedValue, h.validators.Len())
		for i := range pairs {
			pairs[i] = weightedSeq{
				seq:    h.globalMatrix.Row(validatorIdx)[i],
				weight: h.validators.GetWeightByIdx(idx.Validator(i)),
			}
		}
		sort.Slice(pairs, func(i, j int) bool {
			a, b := pairs[i].(weightedSeq), pairs[j].(weightedSeq)
			return a.seq > b.seq
		})
		median := wmedian.Of(pairs, h.validators.Quorum())
		h.globalMedianSeqs[validatorIdx] = median.(weightedSeq).seq
	}
	// invalidate search strategy cache
	cache := NewMetricFnCache(h.GetMetricOf, 128)
	h.searchStrategy = NewMetricStrategy(cache.GetMetricOf)
	h.dirty = false
}

func (h *QuorumIndexer) Choose(existingParents hash.Events, options hash.Events) int {
	metrics := make([]RootProgressMetrics, len(options))
	// first get metrics of each options
	metrics = h.GetMetricsOfRootProgress(options, existingParents) //should call GetMetricsofRootProgress
	if metrics == nil {
		// this occurs if all head options are at a previous frame, and thus cannot progress the production of a root in the current frame
		// in this case return a random head
		// +++todo, instead perhaps choose a head that will benefit other validators, or a head that is fast to communicate with (direct P2P peer?)
		return h.r.Intn(len(options))
	}
	// now sort options based on metrics in order of importance
	sort.Sort(sortedRootProgressMetrics(metrics))

	// return the index of the option with the best metrics
	// Note that if the frame of the creator combined with already selected heads
	//+++todo, perhaps include a bias for low latency parents (i.e. P2P peers)
	return metrics[0].idx
}

//below Len, Swap and Less are for implementing the inbuilt sort interface for RootProgressMetrics

func (m sortedRootProgressMetrics) Len() int {
	return len(m)
}

func (m sortedRootProgressMetrics) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m sortedRootProgressMetrics) Less(i, j int) bool {
	// sort RootProgressMetrics based on each metric field
	// if m[i].HeadFrame != m[j].HeadFrame {
	// 	return m[i].HeadFrame > m[j].HeadFrame
	// }

	if m[i].NewFCWeight.Sum() != m[j].NewFCWeight.Sum() {
		return m[i].NewFCWeight.Sum() > m[j].NewFCWeight.Sum()
	}

	if m[i].NewRootKnowledge != m[j].NewRootKnowledge {
		return m[i].NewRootKnowledge > m[j].NewRootKnowledge
	}

	if m[i].NewObservedRootWeight.Sum() != m[j].NewObservedRootWeight.Sum() {
		return m[i].NewObservedRootWeight.Sum() > m[j].NewObservedRootWeight.Sum()
	}
	return true
}

func (h *QuorumIndexer) newRootProgressMetrics(headIdx int) RootProgressMetrics {
	var metric RootProgressMetrics
	metric.NewRootKnowledge = 0
	metric.idx = headIdx
	metric.NewObservedRootWeight = *h.validators.NewCounter()
	metric.NewFCWeight = *h.validators.NewCounter()
	return metric
}

func (h *QuorumIndexer) GetMetricsOfRootProgress(heads hash.Events, chosenHeads hash.Events) []RootProgressMetrics {
	// This function is indended to be used in the process of
	// selecting event block parents from a set of head options.
	// This function returns useful metrics for assessing
	// how much a validator will progress toward producing a root when using head as a parent.
	// creator denotes the validator creating a new event block.
	// chosenHeads are heads that have already been selected
	// head denotes the event block of another validator that is being considered as a potential parent.

	// find max frame number of self event block, and chosen heads
	currentFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find frame number of each head, and max frame number
	var maxHeadFrame idx.Frame = currentFrame
	headFrame := make([]idx.Frame, len(heads))

	for i, head := range heads {
		headFrame[i] = h.Dagi.GetEvent(head).Frame()
		if headFrame[i] > maxHeadFrame {
			maxHeadFrame = headFrame[i]
		}
	}

	for _, head := range chosenHeads {
		if h.Dagi.GetEvent(head).Frame() > maxHeadFrame {
			maxHeadFrame = h.Dagi.GetEvent(head).Frame()
		}
	}

	// only retain heads with max frame number
	var rootProgressMetrics []RootProgressMetrics
	var maxHeads hash.Events
	for i, head := range heads {
		if headFrame[i] >= maxHeadFrame {
			rootProgressMetrics = append(rootProgressMetrics, h.newRootProgressMetrics(i))
			maxHeads = append(maxHeads, head)
		}
	}

	maxFrameRoots := h.lachesis.Store.GetFrameRoots(maxHeadFrame)
	CurrentRootKnowledge := make([]KIdx, len(maxFrameRoots))
	HeadsRootKnowledge := make([]sortedKIdx, len(maxHeads))
	for i, _ := range HeadsRootKnowledge {
		HeadsRootKnowledge[i] = make([]KIdx, len(maxFrameRoots))
	}
	for j, root := range maxFrameRoots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, maxHeads, chosenHeads)
		currentFCProgress := FCProgress[len(FCProgress)-1]

		if currentFCProgress.Sum() <= h.validators.Quorum() {
			CurrentRootKnowledge[j].K = float64(currentFCProgress.Sum())
		} else {
			CurrentRootKnowledge[j].K = float64(h.validators.Quorum())
		}
		CurrentRootKnowledge[j].Root = root

		for i, _ := range maxHeads {
			// Below metrics are computed in order of importance (most important first)
			if FCProgress[i].HasQuorum() && !currentFCProgress.HasQuorum() {
				// This means the root forkless causes the creator when head is a parent, but does not forkless cause without the head
				rootProgressMetrics[i].NewFCWeight.Count(root.Slot.Validator)
			}

			if FCProgress[i].Sum() <= h.validators.Quorum() {
				HeadsRootKnowledge[i][j].K = float64(FCProgress[i].Sum())
			} else {
				HeadsRootKnowledge[i][j].K = float64(h.validators.Quorum())
			}
			HeadsRootKnowledge[i][j].Root = root

			if FCProgress[i].Sum() > 0 && currentFCProgress.Sum() == 0 {
				// this means that creator with head parent observes the root, but creator on its own does not
				// i.e. this is a new root observed via the head
				rootProgressMetrics[i].NewObservedRootWeight.Count(root.Slot.Validator)
			}

		}
	}

	sort.Sort(sortedKIdx(CurrentRootKnowledge))
	var currentKnowledge uint64 = 0
	var bestRootsStake pos.Weight = 0
	for _, kidx := range CurrentRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			currentKnowledge += uint64(kidx.K) * uint64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			currentKnowledge += uint64(kidx.K) * uint64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	for i, _ := range maxHeads {
		sort.Sort(HeadsRootKnowledge[i])
		rootProgressMetrics[i].NewRootKnowledge = 0
		var bestRootsStake pos.Weight = 0
		for _, kidx := range HeadsRootKnowledge[i] {
			rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
			rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
			if bestRootsStake >= h.validators.Quorum() {
				break
			} else if bestRootsStake+rootStake <= h.validators.Quorum() {
				rootProgressMetrics[i].NewRootKnowledge += uint64(kidx.K) * uint64(rootStake)
				bestRootsStake += rootStake
			} else {
				partialStake := h.validators.Quorum() - bestRootsStake
				rootProgressMetrics[i].NewRootKnowledge += uint64(kidx.K) * uint64(partialStake)
				bestRootsStake += partialStake // this should trigger the break condition above
			}
		}
		rootProgressMetrics[i].NewRootKnowledge -= currentKnowledge
	}
	return rootProgressMetrics
}

func (h *QuorumIndexer) GetMetricsOfRootProgressOLD(heads hash.Events, chosenHeads hash.Events) []RootProgressMetrics {
	// This function is indended to be used in the process of
	// selecting event block parents from a set of head options.
	// This function returns useful metrics for assessing
	// how much a validator will progress toward producing a root when using head as a parent.
	// creator denotes the validator creating a new event block.
	// chosenHeads are heads that have already been selected
	// head denotes the event block of another validator that is being considered as a potential parent.

	// find max frame number of self event block, and chosen heads
	currentFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find frame number of each head, and max frame number
	var maxHeadFrame idx.Frame = currentFrame
	headFrame := make([]idx.Frame, len(heads))

	for i, head := range heads {
		headFrame[i] = h.Dagi.GetEvent(head).Frame()
		if headFrame[i] > maxHeadFrame {
			maxHeadFrame = headFrame[i]
		}
	}

	for _, head := range chosenHeads {
		if h.Dagi.GetEvent(head).Frame() > maxHeadFrame {
			maxHeadFrame = h.Dagi.GetEvent(head).Frame()
		}
	}

	// only retain heads with max frame number
	var rootProgressMetrics []RootProgressMetrics
	var maxHeads hash.Events
	for i, head := range heads {
		if headFrame[i] >= maxHeadFrame {
			rootProgressMetrics = append(rootProgressMetrics, h.newRootProgressMetrics(i))
			maxHeads = append(maxHeads, head)
		}
	}
	// +++ToDo only retain chosenHeads with max frame number

	maxFrameRoots := h.lachesis.Store.GetFrameRoots(maxHeadFrame)
	//+++todo, does Store.GetFrameRoots return roots for an undecided frame (or only for decided frames)? If no, need to get roots elsewhere
	for _, root := range maxFrameRoots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, maxHeads, chosenHeads)

		currentFCProgress := FCProgress[len(FCProgress)-1]
		for i, _ := range maxHeads {
			// Below metrics are computed in order of importance (most important first)
			if FCProgress[i].HasQuorum() && !currentFCProgress.HasQuorum() {
				// This means the root forkless causes the creator when head is a parent, but does not forkless cause without the head
				rootProgressMetrics[i].NewFCWeight.Count(root.Slot.Validator)
			}

			if !FCProgress[i].HasQuorum() {
				// if the root does not forkless cause even with the head, add improvement head makes toward forkless cause
				rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
				rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
				rootProgressMetrics[i].NewRootKnowledge += uint64(rootStake) * uint64(FCProgress[i].Sum()-currentFCProgress.Sum())

				// rootProgressMetrics[i].NewRootKnowledge += FCProgress[i].Sum() - currentFCProgress.Sum()

				if FCProgress[i].Sum() > 0 && currentFCProgress.Sum() == 0 {
					// this means that creator with head parent observes the root, but creator on its own does not
					// i.e. this is a new root observed via the head
					rootProgressMetrics[i].NewObservedRootWeight.Count(root.Slot.Validator)
				}
			}
		}
	}
	return rootProgressMetrics
}

func maxFrame(a idx.Frame, b idx.Frame) idx.Frame {
	if a > b {
		return a
	}
	return b
}

func (m sortedKIdx) Len() int {
	return len(m)
}

func (m sortedKIdx) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m sortedKIdx) Less(i, j int) bool {
	return m[i].K > m[j].K
}

func (h *QuorumIndexer) PrintSubgraphK(frame idx.Frame, event hash.Event) {

	//put all events in chosenRoot's subgraph into a buffer
	h.eventRootKnowledgeQByStake(event)
	var eventBuffer dag.Events
	eventBuffer = append(eventBuffer, h.Dagi.GetEvent(event))

	idx := 0
	for {
		l := len(eventBuffer) - 1
		if idx > l {
			break
		}
		e := eventBuffer[idx]

		parents := e.Parents()
		for _, p := range parents {
			pDag := h.Dagi.GetEvent(p)
			if pDag.Frame() == frame {
				eventBuffer = append(eventBuffer, pDag)
			}
		}
		idx++
	}

	//get set of unique events
	events := make(map[dag.Event]bool)
	for _, event := range eventBuffer {
		events[event] = true
	}

	//calculate k for each event in buffer
	allK := make([]float64, len(events))
	i := 0
	for event := range events {
		// allK[i] = h.eventRootKnowledgeQ((event.ID()))
		// allK[i] = h.eventRootKnowledge((event.ID()))
		// frame = h.Dagi.GetEvent(chosenRoot).Frame()
		// FC, _ := h.eventRootKnowledgeByCount(frame-1, (event.ID()), nil)
		// allK[i] = float64(FC.Sum())
		// frame = h.Dagi.GetEvent(chosenRoot).Frame()
		_, allK[i] = h.eventRootKnowledgeQByCount(frame, event.ID(), nil)
		i++
	}
	sort.Float64s(allK)
	for _, k := range allK {
		fmt.Print(",", k)
	}
}

func (h *QuorumIndexer) eventRootKnowledge(event hash.Event) float64 {
	frame := h.Dagi.GetEvent(event).Frame()
	roots := h.lachesis.Store.GetFrameRoots(frame)
	D := float64(h.validators.TotalWeight()) * float64(h.validators.TotalWeight())

	// calculate k for event under consideration

	kNew := 0.0
	for _, root := range roots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, nil) //compute for new event
		kNew += float64(rootStake) * float64(FCProgress[0].Sum())
	}

	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) eventRootKnowledgeByCount(frame idx.Frame, event hash.Event, chosenHeads hash.Events) (*pos.WeightCounter, float64) {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	D := float64(h.validators.Len()) * float64(h.validators.Len())

	// calculate k for event under consideration

	kNew := 0.0
	FCroots := h.validators.NewCounter()
	for _, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		kNew += float64(FCProgress[0].NumCounted())

		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		if FCProgress[0].HasQuorum() {
			FCroots.CountByIdx(rootValidatorIdx)
		}
	}

	kNew = kNew / D

	return FCroots, kNew
}

func (h *QuorumIndexer) eventRootKnowledgeQByCount(frame idx.Frame, event hash.Event, chosenHeads hash.Events) (*pos.WeightCounter, float64) {

	roots := h.lachesis.Store.GetFrameRoots(frame)

	weights := h.validators.SortedWeights()
	ids := h.validators.SortedIDs()
	FCroots := h.validators.NewCounter()
	// calculate k by count for event under consideration

	RootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		if FCProgress[0].HasQuorum() {
			RootKnowledge[i].K = 1.0
			rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
			FCroots.CountByIdx(rootValidatorIdx)
		} else {
			numCounted := FCProgress[0].NumCounted()
			numForQ := FCProgress[0].NumCounted()
			stake := FCProgress[0].Sum()
			for j, weight := range weights {
				if stake >= h.validators.Quorum() {
					break
				}
				if FCProgress[0].Count(ids[j]) {
					stake += weight
					numForQ++
				}

			}
			RootKnowledge[i].K = float64(numCounted) / float64(numForQ)
		}
		RootKnowledge[i].Root = root

	}

	sort.Sort(sortedKIdx(RootKnowledge))
	var kNew float64 = 0

	var bestRootsStake pos.Weight = 0
	rootValidators := make([]idx.Validator, 0)
	numRootsForQ := 0.0
	for _, kidx := range RootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			kNew += kidx.K
			bestRootsStake += rootStake
			numRootsForQ++
			rootValidators = append(rootValidators, rootValidatorIdx)
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			kNew += kidx.K
			bestRootsStake += partialStake // this should trigger the break condition above
			numRootsForQ++
			rootValidators = append(rootValidators, rootValidatorIdx)
		}
	}

	for i, weight := range weights {
		if bestRootsStake >= h.validators.Quorum() {
			break
		}
		notCounted := true
		for _, rootValidator := range rootValidators {
			if ids[i] == idx.ValidatorID(rootValidator) {
				notCounted = false
				break
			}
		}
		if notCounted {
			bestRootsStake += weight
			numRootsForQ++
		}
	}
	// if FCroots.HasQuorum() {
	// 	fmt.Println("Q")
	// }
	return FCroots, kNew / numRootsForQ
}

func (h *QuorumIndexer) eventRootKnowledgeQByStake(event hash.Event) float64 {
	frame := h.Dagi.GetEvent(event).Frame()
	roots := h.lachesis.Store.GetFrameRoots(frame)
	Q := float64(h.validators.Quorum())
	D := (Q * Q)

	// calculate k for event under consideration

	RootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, nil) //compute for new event
		if FCProgress[0].Sum() <= h.validators.Quorum() {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
			RootKnowledge[i].K = float64(FCProgress[0].Sum())
		} else {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			RootKnowledge[i].K = float64(h.validators.Quorum())
		}
		RootKnowledge[i].Root = root

	}

	sort.Sort(sortedKIdx(RootKnowledge))
	var kNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range RootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			kNew += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			kNew += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) LogisticTimingConditionByCount(chosenHeads hash.Events, nParents int, receivedStake pos.Weight) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	// calculate k for new event under consideration

	// FCProgress, kNew := h.eventRootKnowledgeByCount(frame, h.SelfParentEvent, chosenHeads)
	// _, kPrev := h.eventRootKnowledgeByCount(frame, h.SelfParentEvent, nil)
	// FCProgress, kNew := h.eventRootKnowledgeQByCount(frame, h.SelfParentEvent, chosenHeads)
	_, kNew := h.eventRootKnowledgeQByCount(frame, h.SelfParentEvent, chosenHeads)
	_, kPrev := h.eventRootKnowledgeQByCount(frame, h.SelfParentEvent, nil)

	sortedsWeights := h.validators.SortedWeights()

	var numNodesForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range sortedsWeights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			numNodesForQ++
		}
	}
	// change this numnodes for Q to enforce including self?

	// n := float64(len(sortedsWeights))

	delK := 1.0 / (numNodesForQ * numNodesForQ)
	// delK := (2.0*(float64(nParents)-1.0) + 1) / (numNodesForQ * numNodesForQ)
	KStart := 1.0 * delK
	KEnd := 1.0 //- delK

	logKStartKEnd := math.Log(KEnd / KStart)
	// logKStartKEnd := math.Log((1.0 - KStart) / (1.0 - KEnd) * KEnd / KStart)

	// framesPerEvent := 1.0 / (math.Log(n)/math.Log(float64(nParents)) + math.Log(n)/math.Log(float64(nParents)))
	framesPerEvent := 1.0 / (math.Log(numNodesForQ)/math.Log(float64(nParents)) + math.Log(numNodesForQ)/math.Log(float64(nParents)))
	// eventsPerFrame := float64(n) / framesPerEvent
	eventsPerFrame := 1.0 / framesPerEvent
	tau := eventsPerFrame / logKStartKEnd

	delt := tau * framesPerEvent * logKStartKEnd

	tPrev := -tau * math.Log((1.0-kPrev)/kPrev)
	tCond := tPrev + delt
	KCond := 1.0 / (1.0 + math.Exp(-tCond/tau))

	// KCond := kPrev * math.Exp(delt/tau)
	// trunc := delK
	// KCond = math.Trunc(KCond/trunc) * trunc
	// if FCProgress.HasQuorum() {
	// 	fmt.Print(", ", kNew)
	// 	return true // if new event under consideration will be a root of a new frame, create the event
	// }

	if kNew >= KCond {
		// if kNew == kPrev {
		// 	return false
		// }
		// 	// if forklessCausingStake.HasQuorum() {
		// 	// fmt.Print("New Root", h.SelfParentEvent.FullID())
		// 	// }
		fmt.Print(", ", kNew)
		// 	// fmt.Print(", ", FCProgress.Sum())
		// 	// fmt.Println(" ", h.SelfParentEvent.FullID())
		// 	// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		return true
	}

	// if kNew >= 1.0-float64(nParents-1)*delK && kNew != kPrev {
	// 	fmt.Print(", ", kNew)
	// 	return true
	// }
	// fmt.Print(", ", floatNew)
	//find selfParent of selfParent
	// prevSelfEvent := h.SelfParentEvent
	// eventCount := 1
	// for {
	// 	parents := h.Dagi.GetEvent(prevSelfEvent).Parents()
	// 	if len(parents) == 0 {
	// 		break
	// 	}
	// 	noSelfParent := true
	// 	for _, parent := range parents {
	// 		if h.Dagi.GetEvent(parent).Creator() == h.Dagi.GetEvent(h.SelfParentEvent).Creator() {
	// 			prevSelfEvent = parent
	// 			noSelfParent = false
	// 			break
	// 		}
	// 	}
	// 	if noSelfParent {
	// 		break
	// 	}
	// 	if h.Dagi.GetEvent(prevSelfEvent).Frame() <= frame-1 {
	// 		break
	// 	}
	// 	eventCount++
	// }
	// KCond = KStart * math.Pow(math.Exp(delt/tau), float64(eventCount))

	// if kNew > KCond {
	// 	fmt.Print(", ", kNew, ">", KCond, " ", h.Dagi.GetEvent(h.SelfParentEvent).Creator())
	// 	return true
	// }

	// prevSelfEvent := h.SelfParentEvent
	// parents := h.Dagi.GetEvent(prevSelfEvent).Parents()
	// for _, parent := range parents {
	// 	if h.Dagi.GetEvent(parent).Creator() == h.Dagi.GetEvent(h.SelfParentEvent).Creator() {
	// 		prevSelfEvent = parent
	// 		break
	// 	}
	// }
	// if h.Dagi.GetEvent(prevSelfEvent).Frame() == frame {
	// 	_, kPrev = h.eventRootKnowledgeQByCount(frame, prevSelfEvent, nil)
	// 	KCond = kPrev * math.Exp(delt/tau) * math.Exp(delt/tau)

	// 	if kNew > KCond {
	// 		fmt.Print(", ", kNew)
	// 		return true
	// 	}
	// }

	return false
}
func (h *QuorumIndexer) ExponentialTimingConditionByCount(chosenHeads hash.Events, nParents int, receivedStake pos.Weight) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	// calculate k for new event under consideration

	// FCProgress, kNew := h.eventRootKnowledgeByCount(frame, h.SelfParentEvent, chosenHeads)
	// _, kPrev := h.eventRootKnowledgeByCount(frame, h.SelfParentEvent, nil)
	FCProgress, kNew := h.eventRootKnowledgeQByCount(frame, h.SelfParentEvent, chosenHeads)
	_, kPrev := h.eventRootKnowledgeQByCount(frame, h.SelfParentEvent, nil)

	weights := h.validators.SortedWeights()

	var numNodesForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range weights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			numNodesForQ++
		}
	}

	n := float64(len(weights))

	delK := 1.0 / (numNodesForQ * numNodesForQ)
	// delK := 1.0 / (n * n)
	KStart := 1 * delK
	KEnd := 1.0

	logKStartKEnd := math.Log(KEnd / KStart)

	// framesPerEvent := 0.045
	framesPerEvent := 1.0 / (math.Log(n)/math.Log(float64(nParents)) + math.Log(n)/math.Log(float64(nParents)))
	eventsPerFrame := float64(len(weights)) / framesPerEvent
	tau := eventsPerFrame / logKStartKEnd

	delt := tau * framesPerEvent * logKStartKEnd

	KCond := kPrev * math.Exp(delt/tau)
	// trunc := delK
	// KCond = math.Trunc(KCond/trunc) * trunc
	if FCProgress.HasQuorum() {
		// 	fmt.Print("New Root")
		// fmt.Print(", ", floatNew)
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		fmt.Print(", ", kNew)
		// fmt.Print(", ", FCProgress.Sum())
		return true // if new event under consideration will be a root of a new frame, create the event
	}

	if kNew >= KCond {
		// if kNew == kPrev {
		// 	return false
		// }
		// 	// if forklessCausingStake.HasQuorum() {
		// 	// fmt.Print("New Root", h.SelfParentEvent.FullID())
		// 	// }
		fmt.Print(", ", kNew)
		// 	// fmt.Print(", ", FCProgress.Sum())
		// 	// fmt.Println(" ", h.SelfParentEvent.FullID())
		// 	// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		return true
	}

	// if kNew >= 1.0-float64(nParents-1)*delK && kNew != kPrev {
	// 	fmt.Print(", ", kNew)
	// 	return true
	// }
	// fmt.Print(", ", floatNew)
	//find selfParent of selfParent
	// prevSelfEvent := h.SelfParentEvent
	// eventCount := 1
	// for {
	// 	parents := h.Dagi.GetEvent(prevSelfEvent).Parents()
	// 	if len(parents) == 0 {
	// 		break
	// 	}
	// 	noSelfParent := true
	// 	for _, parent := range parents {
	// 		if h.Dagi.GetEvent(parent).Creator() == h.Dagi.GetEvent(h.SelfParentEvent).Creator() {
	// 			prevSelfEvent = parent
	// 			noSelfParent = false
	// 			break
	// 		}
	// 	}
	// 	if noSelfParent {
	// 		break
	// 	}
	// 	if h.Dagi.GetEvent(prevSelfEvent).Frame() <= frame-1 {
	// 		break
	// 	}
	// 	eventCount++
	// }
	// KCond = KStart * math.Pow(math.Exp(delt/tau), float64(eventCount))

	// if kNew > KCond {
	// 	fmt.Print(", ", kNew, ">", KCond, " ", h.Dagi.GetEvent(h.SelfParentEvent).Creator())
	// 	return true
	// }

	// prevSelfEvent := h.SelfParentEvent
	// parents := h.Dagi.GetEvent(prevSelfEvent).Parents()
	// for _, parent := range parents {
	// 	if h.Dagi.GetEvent(parent).Creator() == h.Dagi.GetEvent(h.SelfParentEvent).Creator() {
	// 		prevSelfEvent = parent
	// 		break
	// 	}
	// }
	// if h.Dagi.GetEvent(prevSelfEvent).Frame() == frame {
	// 	_, kPrev = h.eventRootKnowledgeQByCount(frame, prevSelfEvent, nil)
	// 	KCond = kPrev * math.Exp(delt/tau) * math.Exp(delt/tau)

	// 	if kNew > KCond {
	// 		fmt.Print(", ", kNew)
	// 		return true
	// 	}
	// }

	return false
}

func (h *QuorumIndexer) ExponentialTimingCondition(chosenHeads hash.Events, nParents int, receivedStake pos.Weight) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	forklessCausingStake := h.validators.NewCounter()

	roots := h.lachesis.Store.GetFrameRoots(frame)
	Q := float64(h.validators.Quorum())
	D := (Q * Q) //- float64(minWeight)*float64(minWeight)
	// calculate k for new event under consideration

	NewRootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		// rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)

		newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event
		if newFCProgress[0].Sum() <= h.validators.Quorum() {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
			NewRootKnowledge[i].K = float64(newFCProgress[0].Sum())
		} else {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			NewRootKnowledge[i].K = float64(h.validators.Quorum())
		}
		NewRootKnowledge[i].Root = root

		if newFCProgress[0].HasQuorum() {
			forklessCausingStake.CountByIdx(rootValidatorIdx)
		}
	}

	sort.Sort(sortedKIdx(NewRootKnowledge))
	var kNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range NewRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			kNew += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			kNew += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	kNew = kNew / D

	// Calculate k for chosenHeads
	kChosenHeads := make([]float64, len(chosenHeads))
	chosenHeads = hash.Events{h.SelfParentEvent}
	for j, head := range chosenHeads {
		if h.Dagi.GetEvent(head).Frame() == frame {
			RootKnowledge := make([]KIdx, len(roots))
			for i, root := range roots {
				FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(head, root.ID, nil, nil) //compute for new event
				if FCProgress[0].Sum() <= h.validators.Quorum() {
					// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
					RootKnowledge[i].K = float64(FCProgress[0].Sum())
				} else {
					// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
					RootKnowledge[i].K = float64(h.validators.Quorum())
				}
				RootKnowledge[i].Root = root

			}
			kChosenHeads[j] = 0.0
			sort.Sort(sortedKIdx(RootKnowledge))
			var bestRootsStake pos.Weight = 0
			for _, kidx := range RootKnowledge {
				rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
				rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
				if bestRootsStake >= h.validators.Quorum() {
					break
				} else if bestRootsStake+rootStake <= h.validators.Quorum() {
					kChosenHeads[j] += float64(kidx.K) * float64(rootStake)
					bestRootsStake += rootStake
				} else {
					partialStake := h.validators.Quorum() - bestRootsStake
					kChosenHeads[j] += float64(kidx.K) * float64(partialStake)
					bestRootsStake += partialStake // this should trigger the break condition above
				}
			}
			kChosenHeads[j] = kChosenHeads[j] / D
		}
	}

	kMaxHead := 0.0
	for _, k := range kChosenHeads {
		if kMaxHead < k {
			kMaxHead = k
		}
	}

	weights := h.validators.SortedWeights()
	minWeight := weights[len(weights)-1]
	// maxWeight := weights[0]

	var numNodesForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range weights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			numNodesForQ++
		}
	}

	n := float64(len(weights))
	// delK := float64(minWeight) * float64(minWeight) / D

	// selfCreator := h.validators.GetIdx(h.Dagi.GetEvent(h.SelfParentEvent).Creator())
	// selfWeight := h.validators.GetWeightByIdx(selfCreator)
	// delK := float64(selfWeight) * float64(selfWeight) / D
	meanWeight := float64(h.validators.TotalWeight()) / n
	delK := float64(meanWeight) * float64(meanWeight) / D
	KStart := 1 * delK
	KEnd := 1.0

	logKStartKEnd := math.Log(KEnd / KStart)

	// framesPerEvent := 0.045
	framesPerEvent := 1.0 / (math.Log(n)/math.Log(float64(nParents)) + math.Log(n)/math.Log(float64(nParents)))
	eventsPerFrame := float64(len(weights)) / framesPerEvent
	tau := eventsPerFrame / logKStartKEnd

	// tPrev := 0.0
	KCond := 0.0
	// tCond := 0.0
	// delt := float64(len(weights))
	// delt := eventsPerFrame / n

	// delt := eventsPerFrame * float64(selfWeight) / float64(h.validators.TotalWeight())
	delt := tau * framesPerEvent * logKStartKEnd
	// delt = float64(receivedStake) / Q
	// if prevFrameBehind {
	// 	// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
	// 	K0 := float64(minWeight) * float64(minWeight) / D
	// 	tK0 := -tau * math.Log(1.0/K0-1.0)
	// 	KRoot := 1.0 - float64(minWeight)*float64(minWeight)/D
	// 	tKroot := -tau * math.Log(1.0/KRoot-1.0)
	// 	tPrev = -tau * math.Log(1.0/floatPrev-1.0)
	// 	tCond = tK0 - (tKroot - tPrev) + delt
	// 	KCond = 1.0 / (1.0 + math.Exp(-tCond/tau))
	// 	// KCond = KCond - (1.0 - floatPrev)
	// } else {
	// tPrev = tau * math.Log(kMaxHead/KStart)
	// tCond = tPrev + delt
	// KCond = KStart * math.Exp(tCond/tau)
	KCond = kMaxHead * math.Exp(delt/tau)
	// }

	// KCond := 1.0 / (1.0 + math.Exp(-(tCond-tHalf)/tau))
	// trunc := float64(minWeight) * float64(minWeight) / D
	// KCond = math.Trunc(KCond/trunc) * trunc

	// if floatNew-floatPrev < delK {
	// 	return false
	// }

	if forklessCausingStake.HasQuorum() {
		// 	fmt.Print("New Root")
		// fmt.Print(", ", floatNew)
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		// fmt.Print(", ", kNew)
		return true // if new event under consideration will be a root of a new frame, create the event
	}

	// if floatPrev == 0 {
	// 	fmt.Println("floatPrev=0")
	// }
	// if floatPrev <= K0 && floatPrev > 0 {
	// 	//need a condition here not to do this more than once per frame!
	// 	if floatNew >= (2*(nParents-1)+1)*K0 {
	// 		fmt.Print(", ", floatNew)
	// 		return true
	// 	}
	// }

	if kNew >= KCond {
		if kNew-kMaxHead < float64(minWeight)*float64(minWeight)/D {
			return false
		}
		// if forklessCausingStake.HasQuorum() {
		// fmt.Print("New Root", h.SelfParentEvent.FullID())
		// }
		// fmt.Print(", ", kNew)
		// fmt.Println(" ", h.SelfParentEvent.FullID())
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		return true
	}
	// fmt.Print(", ", floatNew)
	return false
}

func (h *QuorumIndexer) LogisticTimingCondition4(chosenHeads hash.Events, nParents int, receivedStake pos.Weight) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	forklessCausingStake := h.validators.NewCounter()

	roots := h.lachesis.Store.GetFrameRoots(frame)
	Q := float64(h.validators.Quorum())
	D := (Q * Q) //- float64(minWeight)*float64(minWeight)
	// calculate k for new event under consideration

	NewRootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		// rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)

		newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event
		if newFCProgress[0].Sum() <= h.validators.Quorum() {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
			NewRootKnowledge[i].K = float64(newFCProgress[0].Sum())
		} else {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			NewRootKnowledge[i].K = float64(h.validators.Quorum())
		}
		NewRootKnowledge[i].Root = root

		if newFCProgress[0].HasQuorum() {
			forklessCausingStake.CountByIdx(rootValidatorIdx)
		}
	}

	sort.Sort(sortedKIdx(NewRootKnowledge))
	var kNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range NewRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			kNew += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			kNew += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	kNew = kNew / D

	// Calculate k for chosenHeads
	kChosenHeads := make([]float64, len(chosenHeads))
	for j, head := range chosenHeads {
		if h.Dagi.GetEvent(head).Frame() == frame {
			RootKnowledge := make([]KIdx, len(roots))
			for i, root := range roots {
				FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(head, root.ID, nil, nil) //compute for new event
				if FCProgress[0].Sum() <= h.validators.Quorum() {
					// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
					RootKnowledge[i].K = float64(FCProgress[0].Sum())
				} else {
					// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
					RootKnowledge[i].K = float64(h.validators.Quorum())
				}
				RootKnowledge[i].Root = root

			}
			kChosenHeads[j] = 0.0
			sort.Sort(sortedKIdx(RootKnowledge))
			var bestRootsStake pos.Weight = 0
			for _, kidx := range RootKnowledge {
				rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
				rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
				if bestRootsStake >= h.validators.Quorum() {
					break
				} else if bestRootsStake+rootStake <= h.validators.Quorum() {
					kChosenHeads[j] += float64(kidx.K) * float64(rootStake)
					bestRootsStake += rootStake
				} else {
					partialStake := h.validators.Quorum() - bestRootsStake
					kChosenHeads[j] += float64(kidx.K) * float64(partialStake)
					bestRootsStake += partialStake // this should trigger the break condition above
				}
			}
			kChosenHeads[j] = kChosenHeads[j] / D
		}
	}

	kMaxHead := 0.0
	for _, k := range kChosenHeads {
		if kMaxHead < k {
			kMaxHead = k
		}
	}

	weights := h.validators.SortedWeights()
	minWeight := weights[len(weights)-1]
	// maxWeight := weights[0]

	var numNodesForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range weights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			numNodesForQ++
		}
	}

	delK := float64(minWeight) * float64(minWeight) / D
	KStart := delK
	KEnd := 1.0 - delK

	logKStartKEnd := math.Log((1 - KStart) / (1 - KEnd) * KEnd / KStart)

	framesPerEvent := 1.75
	eventsPerFrame := float64(len(weights)) / framesPerEvent
	tau := eventsPerFrame / logKStartKEnd

	tPrev := 0.0
	KCond := 0.0
	tCond := 0.0
	delt := 1.0
	// delt = float64(receivedStake) / Q
	// if prevFrameBehind {
	// 	// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
	// 	K0 := float64(minWeight) * float64(minWeight) / D
	// 	tK0 := -tau * math.Log(1.0/K0-1.0)
	// 	KRoot := 1.0 - float64(minWeight)*float64(minWeight)/D
	// 	tKroot := -tau * math.Log(1.0/KRoot-1.0)
	// 	tPrev = -tau * math.Log(1.0/floatPrev-1.0)
	// 	tCond = tK0 - (tKroot - tPrev) + delt
	// 	KCond = 1.0 / (1.0 + math.Exp(-tCond/tau))
	// 	// KCond = KCond - (1.0 - floatPrev)
	// } else {
	tPrev = -tau * math.Log(1.0/kMaxHead-1.0)
	tCond = tPrev + delt
	KCond = 1.0 / (1.0 + math.Exp(-tCond/tau))
	// }

	// KCond := 1.0 / (1.0 + math.Exp(-(tCond-tHalf)/tau))
	// trunc := float64(minWeight) * float64(minWeight) / D
	// KCond = math.Trunc(KCond/trunc) * trunc

	// if floatNew-floatPrev < delK {
	// 	return false
	// }

	if forklessCausingStake.HasQuorum() {
		// 	fmt.Print("New Root")
		// fmt.Print(", ", floatNew)
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		fmt.Print(", ", kNew)
		return true // if new event under consideration will be a root of a new frame, create the event
	}

	// if floatPrev == 0 {
	// 	fmt.Println("floatPrev=0")
	// }
	// if floatPrev <= K0 && floatPrev > 0 {
	// 	//need a condition here not to do this more than once per frame!
	// 	if floatNew >= (2*(nParents-1)+1)*K0 {
	// 		fmt.Print(", ", floatNew)
	// 		return true
	// 	}
	// }

	if kNew >= KCond {
		if kNew-kMaxHead < float64(minWeight)*float64(minWeight)/D {
			return false
		}
		// if forklessCausingStake.HasQuorum() {
		// fmt.Print("New Root", h.SelfParentEvent.FullID())
		// }
		fmt.Print(", ", kNew)
		// fmt.Println(" ", h.SelfParentEvent.FullID())
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		return true
	}
	// fmt.Print(", ", floatNew)
	return false
}

func (h *QuorumIndexer) LogisticTimingCondition3(chosenHeads hash.Events, nParents int) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	var selfWeight pos.Weight
	selfCreator := h.validators.GetIdx(h.Dagi.GetEvent(h.SelfParentEvent).Creator())
	selfWeight = h.validators.GetWeightByIdx(selfCreator)

	forklessCausingStake := h.validators.NewCounter()

	roots := h.lachesis.Store.GetFrameRoots(frame)
	NewRootKnowledge := make([]KIdx, len(roots))

	for i, root := range roots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		// rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)

		newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event
		if newFCProgress[0].Sum() <= h.validators.Quorum() {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
			NewRootKnowledge[i].K = float64(newFCProgress[0].Sum())
		} else {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			NewRootKnowledge[i].K = float64(h.validators.Quorum())
		}
		NewRootKnowledge[i].Root = root

		if newFCProgress[0].HasQuorum() {
			forklessCausingStake.CountByIdx(rootValidatorIdx)
		}
	}
	prevFrameBehind := false
	if h.Dagi.GetEvent(h.SelfParentEvent).Frame() < frame {
		prevFrameBehind = true
		roots = h.lachesis.Store.GetFrameRoots(h.Dagi.GetEvent(h.SelfParentEvent).Frame())
	}
	PrevRootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		PrevFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, nil) //compute for new event
		if PrevFCProgress[0].Sum() <= h.validators.Quorum() {
			// PrevRootKnowledge[i].K = uint64(rootStake) * uint64(PrevFCProgress[0].Sum())
			PrevRootKnowledge[i].K = float64(PrevFCProgress[0].Sum())
		} else {
			// PrevRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			PrevRootKnowledge[i].K = float64(h.validators.Quorum())
		}
		PrevRootKnowledge[i].Root = root
	}
	sort.Sort(sortedKIdx(NewRootKnowledge))
	sort.Sort(sortedKIdx(PrevRootKnowledge))

	var floatPrev float64 = 0
	var floatNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range NewRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			floatNew += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			floatNew += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	if prevFrameBehind {
		//new event will be a new root, which will be known by self
		floatNew += float64(selfWeight) * float64(selfWeight)
	}

	bestRootsStake = 0
	for _, kidx := range PrevRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			floatPrev += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			floatPrev += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	Q := float64(h.validators.Quorum())

	weights := h.validators.SortedWeights()
	minWeight := weights[len(weights)-1]
	// maxWeight := weights[0]

	var numNodesForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range weights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			numNodesForQ++
		}
	}

	D := (Q * Q) //- float64(minWeight)*float64(minWeight)
	floatNew = floatNew / D
	floatPrev = floatPrev / D
	// var medianWeight float64 = 0
	// iMedian := len(weights) / 2
	// if len(weights)%2 == 0 {
	// 	medianWeight = float64(weights[iMedian]+weights[iMedian-1]) / 2.0
	// } else {
	// 	medianWeight = float64(weights[iMedian])
	// }
	// n := float64(len(h.validators.IDs()))
	// W := float64(h.validators.TotalWeight())

	// minNodeEvents := 1.0
	// evPerFrame := minNodeEvents * float64(selfWeight) / float64(minWeight)
	// totalEvPerFrame := minNodeEvents * float64(h.validators.TotalWeight()) / float64(minWeight)
	// totalEvPer := minNodeEvents * float64(h.validators.Quorum()) / float64(minWeight)

	total := float64(h.validators.Quorum()) / float64(minWeight)

	delt := total / float64(selfWeight)
	// delt1 := float64(len(weights))
	delt1 := delt
	// delt1 = 1

	// delK := (2*(float64(nParents)-1.0) + 3.0) * float64(minWeight) * float64(minWeight) / D
	delK := float64(minWeight) * float64(minWeight) / D

	KStart := float64(minWeight) * float64(minWeight) / D
	KStart = delK
	// K1 := (2*(float64(nParents)-1.0) + 1.0) * float64(minWeight) * float64(minWeight) / D
	// K1 := (2*(float64(nParents)-1.0) + 3.0) * float64(minWeight) * float64(minWeight) / D
	K1 := 0.234
	delt1 = K1 * Q * 1.05 * (math.Log(numNodesForQ)/math.Log(float64(nParents)) + math.Log(numNodesForQ)/math.Log(float64(nParents)))
	// K1 = 1.0 * float64(minWeight) * float64(minWeight) / D
	// K1 := 7.0 * float64(minWeight) * float64(minWeight) / D
	// K0 := (2 * (float64(nParents) - 1.0)) * float64(minWeight) * float64(minWeight) / D

	// Kroot := (W*Q + (W-Q)*(Q-float64(minWeight))) / (W * W) // this is worst case scenario of requiring maximum root knowledge before producing a new root.

	// Kroot := Q * Q / (W * W)
	// Kroot := 1.0 - K0
	KEnd := 1.0 - float64(minWeight)*float64(minWeight)/D
	KEnd = 1.0 - delK

	logKStartKEnd := math.Log((1 - KStart) / (1 - KEnd) * KEnd / KStart)
	logK1 := math.Log((1 - K1) / K1)

	eventsPerFrame := -delt1 * logKStartKEnd / logK1 / (1 - logKStartKEnd/(2*logK1))
	framesPerEvent := 0.2
	eventsPerFrame = float64(len(weights)) / framesPerEvent
	tau := eventsPerFrame / logKStartKEnd

	// A root will be created when validators observe all roots, and we observe Q of these validators, so number of events needed to create a root is
	// number of events needed to create an event that observes all roots, with exponential growth in number of observed parents
	// according to the number of parents, plus the number of events to observe Q (that observe all roots), again with exponential
	// growth according to the number of parents
	// meanEventsPerFrame := math.Log(numNodesForQ)/math.Log(float64(nParents)) + math.Log(numNodesForQ)/math.Log(float64(nParents))
	// meanEventsPerFrame := math.Log(numNodesForQ) / math.Log(float64(nParents))
	// tauKroot := meanEventsPerFrame * n / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// meanEventsPerFrame := math.Log(nEventsForQ)/math.Log(float64(nParents)) + math.Log(nEventsForQ)/math.Log(float64(nParents))
	// tauKhalf := 1.0 * meanEventsPerFrame * total / math.Log((1-K1)/(1-KHalf)*KHalf/K1)
	// tauFrame := 1.0 * meanEventsPerFrame * total / math.Log((1-K0)/(1-KRoot)*KRoot/K0)
	// tauKhalf := 4.0 * total / math.Log((1-K1)/(1-KHalf)*KHalf/K1)
	// tauKhalf = 2.3 * total / math.Log((1-K1)/(1-KHalf)*KHalf/K1)

	// tauKroot := meanEventsPerFrame * nEventsForQ / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// tauKroot := totalEvPerFrame / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// tHalfKHalf := tauKhalf * math.Log(1.0/K1-1.0)
	// tFrame := tauFrame * math.Log(1.0/K0-1.0)

	// tauKEndMinus2 := (meanEventsPerFrame) * total / math.Log((1-K0)/(1-KEndMinus2)*KEndMinus2/K0)
	// t0KEndMinus2 := tauKEndMinus2 * math.Log(1.0/K0-1.0)

	// K1 := (2*(float64(nParents)-1) + 1) * K0
	// tauK1 := n / math.Log((1-K0)/(1-K1)*K1/K0)
	// tauK1 := numNodesForQ / math.Log((1-K0)/(1-K1)*K1/K0)
	// tauK1 := meanEventsPerFrame * numNodesForQ / math.Log((1-K0)/(1-K1)*K1/K0)
	// t0K1 := tauK1 * math.Log(1.0/K0-1.0)

	// if 1.0/(1.0+math.Exp(-(numNodesForQ-t0Kroot)/tauKroot)) <= K1 {
	// tau := tauKhalf
	// tau := tauFrame
	// tHalf := tHalfKHalf
	// tHalf := tFrame
	// tau = tauKEndMinus2
	// t0 = t0KEndMinus2
	// } else if 1.0/(1.0+math.Exp(-(numNodesForQ-t0K1)/tauK1)) <= Kroot {
	// tau = tauK1
	// t0 = t0K1
	// }
	// t0 := tau*math.Log(1.0/K0-1.0)

	tPrev := 0.0
	KCond := 0.0
	tCond := 0.0
	if prevFrameBehind {
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		K0 := float64(minWeight) * float64(minWeight) / D
		tK0 := -tau * math.Log(1.0/K0-1.0)
		KRoot := 1.0 - float64(minWeight)*float64(minWeight)/D
		tKroot := -tau * math.Log(1.0/KRoot-1.0)
		tPrev = -tau * math.Log(1.0/floatPrev-1.0)
		tCond = tK0 - (tKroot - tPrev) + delt
		KCond = 1.0 / (1.0 + math.Exp(-tCond/tau))
		// KCond = KCond - (1.0 - floatPrev)
	} else {
		tPrev = -tau * math.Log(1.0/floatPrev-1.0)
		tCond = tPrev + delt
		KCond = 1.0 / (1.0 + math.Exp(-tCond/tau))
	}

	// KCond := 1.0 / (1.0 + math.Exp(-(tCond-tHalf)/tau))
	// trunc := float64(minWeight) * float64(minWeight) / D
	// KCond = math.Trunc(KCond/trunc) * trunc

	// if floatNew-floatPrev < delK {
	// 	return false
	// }

	// if forklessCausingStake.HasQuorum() {
	// 	fmt.Print("New Root")
	// fmt.Print(", ", floatNew)
	// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
	// 	return true // if new event under consideration will be a root of a new frame, create the event
	// }

	// if floatPrev == 0 {
	// 	fmt.Println("floatPrev=0")
	// }
	// if floatPrev <= K0 && floatPrev > 0 {
	// 	//need a condition here not to do this more than once per frame!
	// 	if floatNew >= (2*(nParents-1)+1)*K0 {
	// 		fmt.Print(", ", floatNew)
	// 		return true
	// 	}
	// }

	if floatNew+0.0000001 >= KCond {
		if floatNew-floatPrev < float64(minWeight)*float64(minWeight)/D-0.0000001 && !prevFrameBehind {
			return false
		}
		// if forklessCausingStake.HasQuorum() {
		// fmt.Print("New Root", h.SelfParentEvent.FullID())
		// }
		fmt.Print(", ", floatNew)
		// fmt.Println(" ", h.SelfParentEvent.FullID())
		// fmt.Println(" floatPrev:", floatPrev, " floatNew:", floatNew)
		return true
	}
	// fmt.Print(", ", floatNew)
	return false
}

func (h *QuorumIndexer) LogisticTimingCondition2(chosenHeads hash.Events, nParents int) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	var selfWeight pos.Weight
	selfCreator := h.validators.GetIdx(h.Dagi.GetEvent(h.SelfParentEvent).Creator())
	selfWeight = h.validators.GetWeightByIdx(selfCreator)
	// behindFrame := false

	forklessCausingStake := h.validators.NewCounter()
	// Check progress of the new event under consideration

	roots := h.lachesis.Store.GetFrameRoots(frame)
	NewRootKnowledge := make([]KIdx, len(roots))
	PrevRootKnowledge := make([]KIdx, len(roots))
	// if h.Dagi.GetEvent(h.SelfParentEvent).Frame() < frame {
	// 	NewRootKnowledge += uint64(selfWeight) * uint64(selfWeight)
	// 	behindFrame = true
	// }

	for i, root := range roots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		// rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)

		newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event
		if newFCProgress[0].Sum() <= h.validators.Quorum() {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
			NewRootKnowledge[i].K = float64(newFCProgress[0].Sum())
		} else {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			NewRootKnowledge[i].K = float64(h.validators.Quorum())
		}
		NewRootKnowledge[i].Root = root

		if newFCProgress[0].HasQuorum() {
			forklessCausingStake.CountByIdx(rootValidatorIdx)
		}

		PrevFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, nil) //compute for new event
		if PrevFCProgress[0].Sum() <= h.validators.Quorum() {
			// PrevRootKnowledge[i].K = uint64(rootStake) * uint64(PrevFCProgress[0].Sum())
			PrevRootKnowledge[i].K = float64(PrevFCProgress[0].Sum())
		} else {
			// PrevRootKnowledge[i].K = uint64(rootStake) * uint64(h.validators.Quorum())
			PrevRootKnowledge[i].K = float64(h.validators.Quorum())
		}
		PrevRootKnowledge[i].Root = root
	}
	sort.Sort(sortedKIdx(NewRootKnowledge))
	sort.Sort(sortedKIdx(PrevRootKnowledge))

	var floatPrev float64 = 0
	var floatNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range NewRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			floatNew += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			floatNew += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	bestRootsStake = 0
	for _, kidx := range PrevRootKnowledge {
		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
			floatPrev += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.validators.Quorum() - bestRootsStake
			floatPrev += float64(kidx.K) * float64(partialStake)
			bestRootsStake += partialStake // this should trigger the break condition above
		}
	}
	Q := float64(h.validators.Quorum())
	floatNew = floatNew / (Q * Q)
	floatPrev = floatPrev / (Q * Q)
	weights := h.validators.SortedWeights()
	minWeight := weights[len(weights)-1]
	// maxWeight := weights[0]

	var numNodesForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range weights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			numNodesForQ++
		}
	}

	// var medianWeight float64 = 0
	// iMedian := len(weights) / 2
	// if len(weights)%2 == 0 {
	// 	medianWeight = float64(weights[iMedian]+weights[iMedian-1]) / 2.0
	// } else {
	// 	medianWeight = float64(weights[iMedian])
	// }
	// n := float64(len(h.validators.IDs()))
	// W := float64(h.validators.TotalWeight())

	// floatPrev := float64(prevProgress) / (W * W)
	// floatNew := float64(selfProgressRootKnowledge) / (W * W)
	// floatPrev := float64(prevProgress) / (Q * Q)
	// floatNew := float64(selfProgressRootKnowledge) / (Q * Q)

	// minNodeEvents := 1.0
	// evPerFrame := minNodeEvents * float64(selfWeight) / float64(minWeight)
	// totalEvPerFrame := minNodeEvents * float64(h.validators.TotalWeight()) / float64(minWeight)
	// totalEvPer := minNodeEvents * float64(h.validators.Quorum()) / float64(minWeight)

	total := float64(h.validators.Quorum()) / float64(minWeight)

	delt := total / float64(selfWeight)
	// delt := n
	// K0 := float64(minWeight) / (W * W)
	// K0 := 1.0 / (n * n)
	// meanWeight := float64(h.validators.TotalWeight()) / n
	// K0 := meanWeight * meanWeight / (Q * Q)
	// K0 := float64(minWeight) * float64(minWeight) / (Q * Q)
	K0 := (2*(float64(nParents)-1) + 1) / (Q * Q)
	// K0 := float64(maxWeight) * float64(maxWeight) / (Q * Q)
	// K0 := medianWeight * medianWeight / completeKnowledge

	// Kroot := (W*Q + (W-Q)*(Q-float64(minWeight))) / (W * W) // this is worst case scenario of requiring maximum root knowledge before producing a new root.

	// Kroot := Q * Q / (W * W)
	Kroot := 1.0 - K0
	// A root will be created when validators observe all roots, and we observe Q of these validators, so number of events needed to create a root is
	// number of events needed to create an event that observes all roots, with exponential growth in number of observed parents
	// according to the number of parents, plus the number of events to observe Q (that observe all roots), again with exponential
	// growth according to the number of parents
	meanEventsPerFrame := math.Log(numNodesForQ)/math.Log(float64(nParents)) + math.Log(numNodesForQ)/math.Log(float64(nParents))
	// meanEventsPerFrame = 40.0
	// tauKroot := meanEventsPerFrame * n / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// meanEventsPerFrame := math.Log(nEventsForQ)/math.Log(float64(nParents)) + math.Log(nEventsForQ)/math.Log(float64(nParents))
	tauKroot := meanEventsPerFrame * total / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// tauKroot := meanEventsPerFrame * nEventsForQ / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// tauKroot := totalEvPerFrame / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	t0Kroot := tauKroot * math.Log(1.0/K0-1.0)

	// K1 := (2*(float64(nParents)-1) + 1) * K0
	// tauK1 := n / math.Log((1-K0)/(1-K1)*K1/K0)
	// tauK1 := numNodesForQ / math.Log((1-K0)/(1-K1)*K1/K0)
	// t0K1 := tauK1 * math.Log(1.0/K0-1.0)

	var tau float64 = 0
	var t0 float64 = 0
	// if 1.0/(1.0+math.Exp(-(numNodesForQ-t0Kroot)/tauKroot)) <= K1 {
	tau = tauKroot
	t0 = t0Kroot
	// } else if 1.0/(1.0+math.Exp(-(numNodesForQ-t0K1)/tauK1)) <= Kroot {
	// 	tau = tauK1
	// 	t0 = t0K1
	// }
	// t0 := tau*math.Log(1.0/K0-1.0)

	tPrev := t0 - tau*math.Log(1.0/floatPrev-1.0)
	tCond := tPrev + delt

	Kcond := 1.0 / (1.0 + math.Exp(-(tCond-t0)/tau))
	Kcond = math.Trunc(Kcond/K0) * K0

	if forklessCausingStake.HasQuorum() {
		fmt.Print(", ", floatNew)
		return true // if new event under consideration will be a root of a new frame, create the event
	}

	// if floatPrev == 0 {
	// 	fmt.Println("floatPrev=0")
	// }
	// if floatPrev <= K0 && floatPrev > 0 {
	// 	//need a condition here not to do this more than once per frame!
	// 	if floatNew >= (2*(nParents-1)+1)*K0 {
	// 		fmt.Print(", ", floatNew)
	// 		return true
	// 	}
	// }

	if floatNew >= Kcond {
		fmt.Print(", ", floatNew)
		return true
	}

	return false
}

func (h *QuorumIndexer) LogisticTimingCondition(chosenHeads hash.Events, nParents int) bool {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	var selfProgressRootKnowledge uint64 = 0
	headProgressRootKnowledge := make([]uint64, len(chosenHeads))

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	var selfWeight pos.Weight
	selfCreator := h.validators.GetIdx(h.Dagi.GetEvent(h.SelfParentEvent).Creator())
	selfWeight = h.validators.GetWeightByIdx(selfCreator)
	behindFrame := false
	if h.Dagi.GetEvent(h.SelfParentEvent).Frame() < frame {
		selfProgressRootKnowledge += uint64(selfWeight) * uint64(selfWeight)
		behindFrame = true
	}

	forklessCausingStake := h.validators.NewCounter()
	// Check progress of the new event under consideration

	roots := h.lachesis.Store.GetFrameRoots(frame)
	for _, root := range roots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)

		newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event
		selfProgressRootKnowledge += uint64(rootStake) * uint64(newFCProgress[0].Sum())
		if behindFrame && newFCProgress[0].Sum() > 0 {
			selfProgressRootKnowledge += uint64(rootStake) * uint64(selfWeight)
		}
		if behindFrame == false {
			if newFCProgress[0].HasQuorum() {
				forklessCausingStake.CountByIdx(rootValidatorIdx)
			}
		}
		for i, head := range chosenHeads {
			if h.Dagi.GetEvent(head).Frame() == frame {
				headFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(head, root.ID, nil, nil) //compute for new event
				headProgressRootKnowledge[i] += uint64(rootStake) * uint64(headFCProgress[0].Sum())
			}
		}
	}

	var prevProgress uint64 = 0
	for i, head := range chosenHeads {
		if head == h.SelfParentEvent {
			prevProgress = headProgressRootKnowledge[i]
		}
	}

	weights := h.validators.SortedWeights()
	minWeight := weights[0]

	var nEventsForQ float64 = 0
	var Qtest pos.Weight
	for _, weight := range weights {
		if Qtest < h.validators.Quorum() {
			Qtest += weight
			nEventsForQ++
		}
		if minWeight > weight {
			minWeight = weight
		}
	}

	// var medianWeight float64 = 0
	// iMedian := len(weights) / 2
	// if len(weights)%2 == 0 {
	// 	medianWeight = float64(weights[iMedian]+weights[iMedian-1]) / 2.0
	// } else {
	// 	medianWeight = float64(weights[iMedian])
	// }
	n := float64(len(h.validators.IDs()))
	W := float64(h.validators.TotalWeight())
	Q := float64(h.validators.Quorum())
	floatPrev := float64(prevProgress) / (W * W)
	floatNew := float64(selfProgressRootKnowledge) / (W * W)
	// floatPrev := float64(prevProgress) / (Q * Q)
	// floatNew := float64(selfProgressRootKnowledge) / (Q * Q)

	minNodeEvents := 1.0
	evPerFrame := minNodeEvents * float64(selfWeight) / float64(minWeight)
	totalEvPerFrame := minNodeEvents * float64(h.validators.TotalWeight()) / float64(minWeight)
	// totalEvPerFrame := minNodeEvents * float64(h.validators.Quorum()) / float64(minWeight)

	delt := totalEvPerFrame / evPerFrame
	// delt := n
	// K0 := float64(minWeight) / (W * W)
	K0 := 1.0
	// meanWeight := float64(h.validators.TotalWeight()) / n
	// K0 := meanWeight * meanWeight / (Q * Q)
	// K0 := medianWeight * medianWeight / (Q * Q)

	K1 := (2*(float64(nParents)-1) + 1) / (n * n)
	K0 = K1

	Kroot := (W*Q + (W-Q)*(Q-float64(minWeight))) / (W * W) // this is worst case scenario of requiring maximum root knowledge before producing a new root.

	// Kroot := Q * Q / (W * W)
	// Kroot = 1.0 - K0
	// A root will be created when validators observe all roots, and we observe Q of these validators, so number of events needed to create a root is
	// number of events needed to create an event that observes all roots, with exponential growth in number of observed parents
	// according to the number of parents, plus the number of events to observe Q (that observe all roots), again with exponential
	// growth according to the number of parents
	// meanEventsPerFrame := math.Log(n)/math.Log(float64(nParents)) + math.Log(nEventsForQ)/math.Log(float64(nParents))
	// meanEventsPerFrame = 40.0
	// tauKroot := meanEventsPerFrame * n / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	meanEventsPerFrame := math.Log(n)/math.Log(float64(nParents)) + math.Log(nEventsForQ)/math.Log(float64(nParents))
	// tauKroot := meanEventsPerFrame * delt / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	tauKroot := meanEventsPerFrame * n / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	// tauKroot := totalEvPerFrame / math.Log((1-K0)/(1-Kroot)*Kroot/K0)
	t0Kroot := tauKroot * math.Log(1.0/K0-1.0)

	// tauK1 := n / math.Log((1-K0)/(1-K1)*K1/K0)
	// tauK1 := nEventsForQ / math.Log((1-K0)/(1-K1)*K1/K0)
	// t0K1 := tauK1 * math.Log(1.0/K0-1.0)

	var tau float64 = 0
	var t0 float64 = 0
	// if 1.0/(1.0+math.Exp(-(nEventsForQ-t0Kroot)/tauKroot)) <= K1 {
	tau = tauKroot
	t0 = t0Kroot
	// } else if 1.0/(1.0+math.Exp(-(nEventsForQ-t0K1)/tauK1)) <= Kroot {
	// tau = tauK1
	// t0 = t0K1
	// }
	// t0 := tau*math.Log(1.0/K0-1.0)

	tPrev := t0 - tau*math.Log(1.0/floatPrev-1.0)
	tCond := tPrev + delt

	Kcond := 1.0 / (1.0 + math.Exp(-(tCond-t0)/tau))
	Kcond = math.Trunc(Kcond/K0) * K0

	if forklessCausingStake.HasQuorum() {
		fmt.Print(", ", floatNew)
		return true // if new event under consideration will be a root of a new frame, create the event
	}

	// if floatPrev == 0 {
	// 	fmt.Println("floatPrev=0")
	// }
	// if floatPrev <= K0 && floatPrev > 0 {
	// 	//need a condition here not to do this more than once per frame!
	// 	if floatNew >= (2*(nParents-1)+1)*K0 {
	// 		fmt.Print(", ", floatNew)
	// 		return true
	// 	}
	// }

	if floatNew >= Kcond {
		fmt.Print(", ", floatNew)
		return true
	}
	return false
}

func (h *QuorumIndexer) updateMetricStats(metric float64) {

	if h.metricStats.expMovAv == 0 {
		h.metricStats.expMovAv = metric
	} else {
		h.metricStats.expMovAv = h.metricStats.expCoeff*metric + (1-h.metricStats.expCoeff)*h.metricStats.expMovAv
	}
	deviation := metric - h.metricStats.expMovAv
	if h.metricStats.expMovVar == 0 {
		h.metricStats.expMovVar = deviation * deviation
	} else {
		h.metricStats.expMovAv = h.metricStats.expCoeff*deviation*deviation + (1-h.metricStats.expCoeff)*h.metricStats.expMovVar
	}

}

func (h *QuorumIndexer) GetMetricOfViaParents(parents hash.Events) Metric {
	if h.dirty {
		h.recacheState()
	}
	vecClock := make([]dagidx.HighestBeforeSeq, len(parents))
	for i, parent := range parents {
		vecClock[i] = h.Dagi.GetMergedHighestBefore(parent)
	}
	var metric Metric
	for validatorIdx := idx.Validator(0); validatorIdx < h.validators.Len(); validatorIdx++ {

		//find the Highest of all the parents
		var update idx.Event
		for i, _ := range parents {
			if seqOf(vecClock[i].Get(validatorIdx)) > update {
				update = seqOf(vecClock[i].Get(validatorIdx))
			}
		}
		current := h.selfParentSeqs[validatorIdx]
		median := h.globalMedianSeqs[validatorIdx]
		metric += h.diffMetricFn(median, current, update, validatorIdx)
	}
	return metric
}

func (h *QuorumIndexer) GetMetricOf(id hash.Event) Metric {
	if h.dirty {
		h.recacheState()
	}
	vecClock := h.Dagi.GetMergedHighestBefore(id)
	var metric Metric
	for validatorIdx := idx.Validator(0); validatorIdx < h.validators.Len(); validatorIdx++ {
		update := seqOf(vecClock.Get(validatorIdx))
		current := h.selfParentSeqs[validatorIdx]
		median := h.globalMedianSeqs[validatorIdx]
		metric += h.diffMetricFn(median, current, update, validatorIdx)
	}
	return metric
}

func (h *QuorumIndexer) SearchStrategy() SearchStrategy {
	if h.dirty {
		h.recacheState()
	}
	return h.searchStrategy
}

func (h *QuorumIndexer) GetGlobalMedianSeqs() []idx.Event {
	if h.dirty {
		h.recacheState()
	}
	return h.globalMedianSeqs
}

func (h *QuorumIndexer) GetGlobalMatrix() Matrix {
	return h.globalMatrix
}

func (h *QuorumIndexer) GetSelfParentSeqs() []idx.Event {
	return h.selfParentSeqs
}
