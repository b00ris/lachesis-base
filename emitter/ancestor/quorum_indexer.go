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
	"github.com/Fantom-foundation/lachesis-base/utils/piecefunc"
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
	NewRootKnowledge      float64 //uint64
}

type ExpMov struct {
	mean     float64 // exponential moving average
	variance float64 // exponential moving variance
	expCoeff float64 // exponential constant
}

type DagIndex interface {
	dagidx.VectorClock
}
type DiffMetricFn func(median, current, update idx.Event, validatorIdx idx.Validator) Metric

type QuorumIndexer struct {
	Dagi       DagIndex
	Validators *pos.Validators

	SelfParentEvent            hash.Event
	SelfParentEventFlag        bool
	validatorHighestEvents     []dag.Event
	validatorHighestEventsTime []int

	lachesis    *abft.Lachesis
	r           *rand.Rand
	TimingStats map[idx.ValidatorID]ExpMov

	CreatorFrame idx.Frame

	globalMatrix     Matrix
	selfParentSeqs   []idx.Event
	globalMedianSeqs []idx.Event
	dirty            bool
	searchStrategy   SearchStrategy

	diffMetricFn DiffMetricFn
}

func NewExpMov() ExpMov {
	return ExpMov{
		mean:     0,
		variance: 0,
		expCoeff: 0.05,
	}
}

func newTimingStats(validators *pos.Validators) *map[idx.ValidatorID]ExpMov {
	TimingStats := make(map[idx.ValidatorID]ExpMov)
	for _, validator := range validators.IDs() {
		TimingStats[validator] = NewExpMov()
	}
	return &TimingStats
}

func NewQuorumIndexer(validators *pos.Validators, dagi DagIndex, diffMetricFn DiffMetricFn, lachesis *abft.Lachesis) *QuorumIndexer {
	return &QuorumIndexer{
		globalMatrix:               NewMatrix(validators.Len(), validators.Len()),
		globalMedianSeqs:           make([]idx.Event, validators.Len()),
		selfParentSeqs:             make([]idx.Event, validators.Len()),
		Dagi:                       dagi,
		Validators:                 validators,
		diffMetricFn:               diffMetricFn,
		dirty:                      true,
		lachesis:                   lachesis,
		r:                          rand.New(rand.NewSource(time.Now().UnixNano())),
		SelfParentEventFlag:        false,
		TimingStats:                *newTimingStats(validators),
		validatorHighestEvents:     make([]dag.Event, validators.Len()),
		validatorHighestEventsTime: make([]int, validators.Len()),
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

func (h *QuorumIndexer) ProcessEvent(event dag.Event, selfEvent bool, time int) {
	vecClock := h.Dagi.GetMergedHighestBefore(event.ID())
	creatorIdx := h.Validators.GetIdx(event.Creator())
	// update global matrix
	for validatorIdx := idx.Validator(0); validatorIdx < h.Validators.Len(); validatorIdx++ {
		seq := seqOf(vecClock.Get(validatorIdx))
		h.globalMatrix.Row(validatorIdx)[creatorIdx] = seq
		if selfEvent {
			h.selfParentSeqs[validatorIdx] = seq
		}
	}
	h.dirty = true
	if h.validatorHighestEvents[creatorIdx] != nil {
		if event.Seq() > h.validatorHighestEvents[creatorIdx].Seq() {
			h.validatorHighestEvents[creatorIdx] = event
			h.validatorHighestEventsTime[creatorIdx] = time
		}
	} else {
		h.validatorHighestEvents[creatorIdx] = event
		h.validatorHighestEventsTime[creatorIdx] = time
	}
}

func (h *QuorumIndexer) recacheState() {
	// update median seqs
	for validatorIdx := idx.Validator(0); validatorIdx < h.Validators.Len(); validatorIdx++ {
		pairs := make([]wmedian.WeightedValue, h.Validators.Len())
		for i := range pairs {
			pairs[i] = weightedSeq{
				seq:    h.globalMatrix.Row(validatorIdx)[i],
				weight: h.Validators.GetWeightByIdx(idx.Validator(i)),
			}
		}
		sort.Slice(pairs, func(i, j int) bool {
			a, b := pairs[i].(weightedSeq), pairs[j].(weightedSeq)
			return a.seq > b.seq
		})
		median := wmedian.Of(pairs, h.Validators.Quorum())
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
	metric.NewObservedRootWeight = *h.Validators.NewCounter()
	metric.NewFCWeight = *h.Validators.NewCounter()
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
	// CurrentRootKnowledge := make([]KIdx, len(maxFrameRoots))
	HeadsRootKnowledge := make([]sortedKIdx, len(maxHeads))
	for i, _ := range HeadsRootKnowledge {
		HeadsRootKnowledge[i] = make([]KIdx, len(maxFrameRoots))
	}
	for i, _ := range maxHeads {
		heads := make([]hash.Event, len(chosenHeads)+1)
		for j, head := range chosenHeads {
			heads[j] = head
		}
		heads[len(heads)-1] = maxHeads[i]
		// rootProgressMetrics[i].NewRootKnowledge = h.EventRootKnowledgeQByCount(maxHeadFrame, h.SelfParentEvent, heads)
		rootProgressMetrics[i].NewRootKnowledge = h.eventRootKnowledgeQByStake(maxHeadFrame, h.SelfParentEvent, heads) //best
		// rootProgressMetrics[i].NewRootKnowledge = h.eventRootKnowledgeByStake(maxHeadFrame, h.SelfParentEvent, heads)
		// rootProgressMetrics[i].NewRootKnowledge = h.EventRootKnowledgeByCount(maxHeadFrame, h.SelfParentEvent, heads)

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
	// This function can be used (in debugging/testing) to display k values of all events in an event's subgraph

	//put all events in event's subgraph into a buffer
	h.eventRootKnowledgeQByStake(frame, event, nil)
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

	//get a set of unique events
	events := make(map[dag.Event]bool)
	for _, event := range eventBuffer {
		events[event] = true
	}

	//calculate k for each event in buffer
	allK := make([]float64, len(events))
	i := 0
	for event := range events {
		allK[i] = h.EventRootKnowledgeQByCount(frame, event.ID(), nil)
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
	D := float64(h.Validators.TotalWeight()) * float64(h.Validators.TotalWeight())

	// calculate k for event under consideration

	kNew := 0.0
	for _, root := range roots {
		rootValidatorIdx := h.Validators.GetIdx(root.Slot.Validator)
		rootStake := h.Validators.GetWeightByIdx(rootValidatorIdx)
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, nil) //compute for new event
		kNew += float64(rootStake) * float64(FCProgress[0].Sum())
	}

	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) eventRootKnowledgeByCount(frame idx.Frame, event hash.Event, chosenHeads hash.Events) (*pos.WeightCounter, float64) {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	D := float64(h.Validators.Len()) * float64(h.Validators.Len())

	// calculate k for event under consideration

	kNew := 0.0
	FCroots := h.Validators.NewCounter()
	for _, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		kNew += float64(FCProgress[0].NumCounted())

		rootValidatorIdx := h.Validators.GetIdx(root.Slot.Validator)
		if FCProgress[0].HasQuorum() {
			FCroots.CountByIdx(rootValidatorIdx)
		}
	}

	kNew = kNew / D

	return FCroots, kNew
}
func (h *QuorumIndexer) EventRootKnowledgeByCount(frame idx.Frame, event hash.Event, chosenHeads hash.Events) float64 {
	roots := h.lachesis.Store.GetFrameRoots(frame)

	// calculate k for event under consideration
	kNew := 0.0
	for _, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		kNew += float64(FCProgress[0].NumCounted())
	}
	return kNew
}
func (h *QuorumIndexer) EventRootKnowledgeByCountOnline(frame idx.Frame, event hash.Event, chosenHeads hash.Events, online map[idx.ValidatorID]bool) float64 {
	roots := h.lachesis.Store.GetFrameRoots(frame)

	// find total stake of online nodes and ensure it meets quourum
	wOnline := h.Validators.NewCounter()
	numOnline := 0
	for ID, isOnline := range online {
		if isOnline {
			numOnline++
			wOnline.Count(ID)
		}
	}

	// if less than quorum are online, add the minimum number of nodes (i.e. largest offline nodes) that need to come online for quourm to be online
	// if !wOnline.HasQuorum() {
	// 	sortedWeights := h.Validators.SortedWeights()
	// 	sortedIDs := h.Validators.SortedIDs()
	// 	for i, _ := range sortedWeights {
	// 		if !wOnline.Count(sortedIDs[i]) {
	// 			numOnline++
	// 		}
	// 		if wOnline.HasQuorum() {
	// 			break
	// 		}
	// 	}
	// }
	D := float64(numOnline) * float64(numOnline)

	// calculate k for event under consideration
	kNew := 0.0
	for _, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		kNew += float64(FCProgress[0].NumCounted())
	}

	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) EventRootKnowledgeQByCount(frame idx.Frame, event hash.Event, chosenHeads hash.Events) float64 {
	// calculate k by count for event under DAG progress event timing consideration
	// event input is the previous self event
	roots := h.lachesis.Store.GetFrameRoots(frame)

	weights := h.Validators.SortedWeights()
	ids := h.Validators.SortedIDs()

	//calculate k_i for each root i
	RootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		if FCProgress[0].HasQuorum() {
			RootKnowledge[i].K = 1.0 //k_i has maximum value of 1 when root i is known by at least a quorum
		} else {
			// root i is known by less than a quorum
			numCounted := FCProgress[0].NumCounted() //the number of nodes that know the root (the numerator of k_i)
			// now find the denominator of k_i; the number of additional nodes needed to for quorum (if any)
			numForQ := FCProgress[0].NumCounted()
			stake := FCProgress[0].Sum()
			for j, weight := range weights {
				if stake >= h.Validators.Quorum() {
					break
				}
				if FCProgress[0].Count(ids[j]) {
					stake += weight
					numForQ++
				}
			}
			RootKnowledge[i].K = float64(numCounted) / float64(numForQ)
		}
		RootKnowledge[i].Root = root // record which root the k_i is for
	}

	//sort roots by k_i value to ge the best roots
	sort.Sort(sortedKIdx(RootKnowledge))
	var kNew float64 = 0

	// sum k_i for the best known roots, to get the numerator of k
	var bestRootsStake pos.Weight = 0
	rootValidators := make([]idx.Validator, 0)
	numRootsForQ := 0.0
	for _, kidx := range RootKnowledge {
		rootValidatorIdx := h.Validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.Validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.Validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.Validators.Quorum() {
			kNew += kidx.K
			bestRootsStake += rootStake
			numRootsForQ++
			rootValidators = append(rootValidators, rootValidatorIdx)
		} else {
			kNew += kidx.K
			bestRootsStake = h.Validators.Quorum() // this will trigger the break condition above
			numRootsForQ++
			rootValidators = append(rootValidators, rootValidatorIdx)
		}
	}

	// calculate how many extra roots are needed for quorum (if any), to get the denominator of k
	for i, weight := range weights {
		if bestRootsStake >= h.Validators.Quorum() {
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
	return kNew / numRootsForQ // this result should be less than or equal to 1
}

func (h *QuorumIndexer) eventRootKnowledgeByStake(frame idx.Frame, event hash.Event, chosenHeads hash.Events) float64 {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	Q := float64(h.Validators.Quorum())
	D := (Q * Q)

	// calculate k for event under consideration

	RootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		rootValidatorIdx := h.Validators.GetIdx(root.Slot.Validator)
		rootStake := h.Validators.GetWeightByIdx(rootValidatorIdx)
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		if FCProgress[0].Sum() <= h.Validators.Quorum() {
			RootKnowledge[i].K = float64(rootStake) * float64(FCProgress[0].Sum())
			// RootKnowledge[i].K = float64(FCProgress[0].Sum())
		} else {
			RootKnowledge[i].K = float64(rootStake) * float64(h.Validators.Quorum())
			// RootKnowledge[i].K = float64(h.Validators.Quorum())
		}
		RootKnowledge[i].Root = root

	}

	sort.Sort(sortedKIdx(RootKnowledge))
	var kNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range RootKnowledge {
		rootValidatorIdx := h.Validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.Validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.Validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.Validators.Quorum() {
			// kNew += float64(kidx.K) * float64(rootStake)
			kNew += float64(kidx.K)
			bestRootsStake += rootStake
		} else {
			partialStake := h.Validators.Quorum() - bestRootsStake
			kNew += float64(kidx.K) * float64(partialStake) / float64(rootStake)
			bestRootsStake = h.Validators.Quorum() // this will trigger the break condition above
		}
	}
	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) eventRootKnowledgeQByStake(frame idx.Frame, event hash.Event, chosenHeads hash.Events) float64 {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	Q := float64(h.Validators.Quorum())
	D := (Q * Q)

	// calculate k for event under consideration

	RootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
		if FCProgress[0].Sum() <= h.Validators.Quorum() {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(newFCProgress[0].Sum())
			RootKnowledge[i].K = float64(FCProgress[0].Sum())
		} else {
			// NewRootKnowledge[i].K = uint64(rootStake) * uint64(h.Validators.Quorum())
			RootKnowledge[i].K = float64(h.Validators.Quorum())
		}
		RootKnowledge[i].Root = root

	}

	sort.Sort(sortedKIdx(RootKnowledge))
	var kNew float64 = 0

	var bestRootsStake pos.Weight = 0
	for _, kidx := range RootKnowledge {
		rootValidatorIdx := h.Validators.GetIdx(kidx.Root.Slot.Validator)
		rootStake := h.Validators.GetWeightByIdx(rootValidatorIdx)
		if bestRootsStake >= h.Validators.Quorum() {
			break
		} else if bestRootsStake+rootStake <= h.Validators.Quorum() {
			kNew += float64(kidx.K) * float64(rootStake)
			bestRootsStake += rootStake
		} else {
			partialStake := h.Validators.Quorum() - bestRootsStake
			kNew += float64(kidx.K) * float64(partialStake)
			bestRootsStake = h.Validators.Quorum() // this will trigger the break condition above
		}
	}
	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) LogisticTimingConditionByCountOnline(chosenHeads hash.Events, nParents int, online map[idx.ValidatorID]bool) (float64, bool) {

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	n := 0.0
	// onlineWeight := .0
	for _, isOnline := range online {
		if isOnline {

			n++
		}
	}

	// +++TODO there is an assumption that online nodes for prev are the same for new and this may not be correct, use cached value?
	kNew := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	kPrev := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, nil, online)
	// kPrev := 0.0
	// for _, head := range chosenHeads {
	// 	kHead := h.EventRootKnowledgeByCountOnline(frame, head, nil, online) // calculate k for most recent self event
	// 	if kHead > kPrev {
	// 		kPrev = kHead
	// 	}

	// }
	kCond := 0.0
	if kPrev == 0 {
		kParent := 1.0
		for _, parent := range chosenHeads {

			if h.Dagi.GetEvent(parent).Frame() == frame {
				k := h.EventRootKnowledgeByCountOnline(frame-1, parent, nil, online)
				if k < kParent {
					kParent = k
				}
			}
		}
		// +++TODO, go back to frame of prev, in case of skipped frames
		kPrev = h.EventRootKnowledgeByCountOnline(frame-1, h.SelfParentEvent, nil, online) // calculate k for most recent self event
		tPrev := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
		tMax := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kParent-1.0)
		kMin := 1 / (n * n)
		tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)
		tCond := tMin - (tMax - tPrev) + 1.0
		kCond = 1.0 / (1.0 + math.Exp(-tCond*math.Log(float64(nParents))))
	} else {
		p := float64(nParents)
		kCond = p * kPrev / (p*kPrev + 1.0 - kPrev)
	}

	if kNew >= kCond {
		// fmt.Print(", ", kNew)
		return kNew, true
	}

	return kNew, false
}

func timingMedianMean(expMovs map[idx.ValidatorID]ExpMov) float64 {
	tempValues := make([]float64, len(expMovs))
	i := 0
	for _, expMov := range expMovs {
		tempValues[i] = expMov.mean
		i++
	}

	sort.Float64s(tempValues)

	var median float64
	l := len(tempValues)
	if l == 0 {
		return 0
	} else if l%2 == 0 {
		median = (tempValues[l/2-1] + tempValues[l/2]) / 2
	} else {
		median = tempValues[l/2]
	}

	return median
}
func (h *QuorumIndexer) GetMetricOfLogisticOLD(chosenHeads hash.Events, nParents int) Metric {
	// this function returns t_k, the logistic time difference between the current event being considered and the previous self event
	// t_k can be thought of as time difference in units of `DAG progress time'

	framePrev := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	frameNew := framePrev
	// find if a head's frame is ahead of prev self event's frame
	for _, head := range chosenHeads {
		frameNew = maxFrame(frameNew, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeQByCount(frameNew, h.SelfParentEvent, chosenHeads) // calculate k for new event under consideration
	tMax := 2 * math.Log(float64(h.Validators.Quorum())) / math.Log(float64(nParents))
	tNew := 0.0
	if kNew < 1.0 {
		tNew = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kNew-1.0)
	} else {
		tNew = tMax
	}

	kPrev := h.EventRootKnowledgeQByCount(framePrev, h.SelfParentEvent, nil) // calculate k for most recent self event

	tPrev := 0.0
	delt_k := 0.0
	if framePrev == frameNew {
		tPrev = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	} else {
		kMin := 1.0 / (float64(h.Validators.Quorum()) * float64(h.Validators.Quorum()))
		tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)
		kPrev = h.EventRootKnowledgeQByCount(framePrev, h.SelfParentEvent, nil) // calculate k for most recent self event
		tPrev = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
		diffFrames := frameNew - framePrev // number of frames prev is behind new
		tFrame := tMax - tMin              // duration of an entire frame
		tPrev = tPrev - tFrame*float64(diffFrames)
	}
	delt_k = tNew - tPrev

	return Metric(delt_k * piecefunc.DecimalUnit)
}
func (h *QuorumIndexer) GetMetricOfLogistic(chosenHeads hash.Events, nParents int, online map[idx.ValidatorID]bool) Metric {
	prevFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	newFrame := prevFrame

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		newFrame = maxFrame(newFrame, h.Dagi.GetEvent(head).Frame())
	}

	n := 0.0
	for _, isOnline := range online {
		if isOnline {
			n++
		}
	}

	// +++TODO there is an assumption that online nodes for prev are the same for new and this may not be correct, use cached value?
	kMin := 1.0 / (n * n)
	kNew := h.EventRootKnowledgeByCountOnline(newFrame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	// +++TODO confirm no problem when kNew=1 so tNew =infinity
	kPrev := h.EventRootKnowledgeByCountOnline(prevFrame, h.SelfParentEvent, nil, online) // calculate k for most recent self event

	tNew := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kNew-1.0)
	tPrev := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)

	delt_k := 0.0
	if prevFrame < newFrame {
		delt_k = tNew - tMin
		// Calculate remaining DAG time for previous event to complete its frame
		roots := h.lachesis.Store.GetFrameRoots(prevFrame + 1)
		kRootMin := 1.0 - kMin
		for _, root := range roots {
			kRoot := h.EventRootKnowledgeByCountOnline(prevFrame, root.ID, nil, online)
			if kRoot < kRootMin {
				kRootMin = kRoot
			}
		}
		tRoot := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kRootMin-1.0)
		if tRoot > tPrev {
			delt_k += tRoot - tPrev
		}

		//if previous is more than one frame behind, calulate DAG time duration of those intervening frames using roots marking the end of each frame

		for frame := prevFrame + 2; frame < newFrame; frame++ {
			kRootMin = 1.0 - kMin
			roots := h.lachesis.Store.GetFrameRoots(frame)
			for _, root := range roots {
				kRoot := h.EventRootKnowledgeByCountOnline(frame, root.ID, nil, online)
				if kRoot < kRootMin {
					kRootMin = kRoot
				}
			}
			tRoot := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kRootMin-1.0)
			delt_k += tRoot - tMin
		}
	} else {
		delt_k = tNew - tPrev

	}
	return Metric(delt_k * piecefunc.DecimalUnit)
}

func (h *QuorumIndexer) ValidatorComparison(chosenHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) (float64, bool) {
	n := float64(h.Validators.Len())
	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	kGreaterCount := 0.0
	kGreaterStake := h.Validators.NewCounter()
	kPrev := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, nil, online)        // calculate k for new event under consideration
	kNew := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration

	// kPrev := h.EventRootKnowledgeQByCount(frame, h.SelfParentEvent, nil)
	// kNew := h.EventRootKnowledgeQByCount(frame, h.SelfParentEvent, chosenHeads)
	// kPrev := h.eventRootKnowledgeQByStake(frame, h.SelfParentEvent, nil)
	// kNew := h.eventRootKnowledgeQByStake(frame, h.SelfParentEvent, chosenHeads)
	if kNew > kPrev {
		for _, e := range h.validatorHighestEvents {
			if e != nil {
				eFrame := h.Dagi.GetEvent(e.ID()).Frame()
				if eFrame > frame {
					// if the highest event of another validator is ahead in frame number it is ahead of self (no need to calculate k)
					kGreaterCount++
					kGreaterStake.Count(e.Creator())
				} else if eFrame == frame {
					k := h.EventRootKnowledgeByCountOnline(frame, e.ID(), nil, online)
					// k := h.eventRootKnowledgeQByStake(frame, e.ID(), nil)
					// k := h.EventRootKnowledgeQByCount(frame, e.ID(), nil)
					if k >= kPrev {
						kGreaterCount++
						kGreaterStake.Count(e.Creator())
					}
				}
			}
		}

		selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()

		sortedIDs := h.Validators.SortedIDs()
		sortedWeights := h.Validators.SortedWeights()
		quorumCounter := h.Validators.NewCounter()
		numQuorum := 0.0
		quorumCounter.Count(selfID) // assume self is a properly functioning validator, count self plus minimum number of other validators for quourm
		maxStake := sortedWeights[len(sortedWeights)-1]
		minStake := sortedWeights[0]
		for i, ID := range sortedIDs {
			if online[ID] {
				if sortedWeights[i] > maxStake {
					maxStake = sortedWeights[i]
				}
				if sortedWeights[i] < minStake {
					minStake = sortedWeights[i]
				}
			}
		}
		for _, ID := range sortedIDs {
			if ID != selfID && online[ID] {
				quorumCounter.Count(ID)
				numQuorum++
			}
			if quorumCounter.HasQuorum() {
				break
			}
		}
		// selfStake := float64(h.Validators.GetWeightByIdx(h.Validators.GetIdx(selfID)))
		// threshParam = threshParam * float64(h.Validators.TotalWeight()) / float64(selfStake) / n // lower threshold for larger stake; more frequent event creation by large validators
		maxThresh := (n - 1.0) / n
		// maxThresh := numQuorum / n
		// maxThresh := threshParam
		// minThresh := 4 / n
		// m := -(maxThresh - minThresh) / (float64(maxStake) - float64(minStake))
		// b := maxThresh - m*float64(minStake)

		// threshParam = m*selfStake + b
		// threshParam = (numQuorum) / n
		// threshParam = 0.2
		if threshParam > maxThresh {
			threshParam = maxThresh
		}
		if threshParam < 1/n {
			threshParam = 1 / n
		}
		if kGreaterCount/n >= threshParam {
			return kGreaterCount / n, true
		}
	}
	return kGreaterCount / n, false
}

func (h *QuorumIndexer) KComparison(passedTime float64, allHeads hash.Events, chosenHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) bool {
	selfFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	count := 0.0
	stake := h.Validators.NewCounter()

	KPrev := h.EventRootKnowledgeMatrix(h.SelfParentEvent, selfFrame)
	for _, e := range h.validatorHighestEvents {
		if e != nil {
			eFrame := h.Dagi.GetEvent(e.ID()).Frame()
			if eFrame > selfFrame {
				// if the highest event of another validator is ahead in frame number it is ahead of self (no need to calculate k)
				count++
				stake.Count(e.Creator())
			} else if eFrame == selfFrame {
				Ke := h.EventRootKnowledgeMatrix(e.ID(), selfFrame)
				_, countDiff, _ := h.RootKnowledgeDifference(Ke, KPrev)
				if countDiff > 0 {
					count++
					stake.Count(e.Creator())
				}
			}
		}
	}
	n := float64(h.Validators.Len())
	if count/n > threshParam {
		return true
	}
	return false
}

func (h *QuorumIndexer) HighestBeforeComparison(passedTime float64, allHeads hash.Events, chosenHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) bool {
	eSelf := h.Dagi.GetEvent(h.SelfParentEvent)
	parents := eSelf.Parents()
	selfHB := h.GetMetricOfViaParents(parents)
	GreaterCount := 0.0
	GreaterStake := h.Validators.NewCounter()
	for i, e := range h.validatorHighestEvents {
		if e != nil {
			HB := h.GetMetricOfViaParents(e.Parents())
			if HB > selfHB {
				GreaterCount++
				e := h.validatorHighestEvents[i]
				GreaterStake.Count(e.Creator())
			}
		}
	}
	n := float64(h.Validators.Len())
	if GreaterCount/n > threshParam {
		return true
	}
	return false
}

func (h *QuorumIndexer) TimeComparison(passedTime float64, allHeads hash.Events, chosenHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) bool {
	selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
	selfIdx := h.Validators.GetIdx(selfID)
	selft := h.validatorHighestEventsTime[selfIdx]
	GreaterCount := 0.0
	GreaterStake := h.Validators.NewCounter()
	for i, t := range h.validatorHighestEventsTime {
		if t >= selft {
			GreaterCount++
			e := h.validatorHighestEvents[i]
			GreaterStake.Count(e.Creator())
		}
	}
	n := float64(h.Validators.Len())
	if GreaterCount/n > threshParam {
		return true
	}
	return false
}

func (h *QuorumIndexer) ValidatorComparisonAndTime(passedTime float64, allHeads hash.Events, chosenHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) bool {

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	kGreaterCount := 0.0
	kGreaterStake := h.Validators.NewCounter()
	kPrev := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, nil, online)        // calculate k for prev self event
	kNew := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	if kNew > kPrev {
		// this condition prevents the function always returning true when self is unable to make DAG progress
		// All new events would eventually have the same k when less than QUORUM nodes are online and would lead to spamming events
		for _, e := range h.validatorHighestEvents {
			if e != nil {
				eFrame := h.Dagi.GetEvent(e.ID()).Frame()
				if eFrame > frame {
					// if the highest event of another validator is ahead in frame number it is ahead of self (no need to calculate k)
					kGreaterCount++
					kGreaterStake.Count(e.Creator())
				} else if eFrame == frame {
					k := h.EventRootKnowledgeByCountOnline(frame, e.ID(), nil, online)
					if k >= kPrev {
						kGreaterCount++
						kGreaterStake.Count(e.Creator())
					}
				}
			}
		}
		n := float64(h.Validators.Len())
		// k0 := threshParam
		// tau := 0.05
		// kGreaterCount = kGreaterCount / n
		// kGreaterCount = 1.0 / (1.0 + math.Exp(-(kGreaterCount-k0)/tau))

		selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
		sortedIDs := h.Validators.SortedIDs()
		quorumCounter := h.Validators.NewCounter()
		numQuorum := 0
		quorumCounter.Count(selfID) // assume self is a properly functioning validator, count self plus minimum number of other validators for quourm
		for _, ID := range sortedIDs {
			if ID != selfID {
				if online[ID] {
					quorumCounter.Count(ID)
					numQuorum++
				}
			}
			if quorumCounter.HasQuorum() {
				break
			}

		}
		// tMax := threshParam
		// gMax := 0.5 * n
		// m := -gMax / tMax
		// if kGreaterCount >= m*passedTime+gMax {
		// 	return true
		// }

		tMax := threshParam
		gMax := n - 1.0
		gMin := 1.0
		m := (gMax - gMin) / tMax
		if kGreaterCount >= m*passedTime+gMin || kGreaterCount >= gMax {
			return true
		}
		// selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
		// selfStake := float64(h.Validators.GetWeightByIdx(h.Validators.GetIdx(selfID)))
		// threshParam = threshParam * float64(h.Validators.TotalWeight()) / float64(selfStake) / n // lower threshold for larger stake; more frequent event creation by large validators
		// if kGreaterCount*passedTime > 110 {
		// 	return true
		// }
	}
	return false
}

func (h *QuorumIndexer) ValidatorComparisonAndTimeOLD(passedTime float64, allHeads hash.Events, chosenHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) bool {
	// +++TODO to improve this function it would be better to cache the highest event from each validator as events are added to the DAG, also cache its k value
	// find max frame of all heads
	var frame idx.Frame
	// for _, head := range allHeads {
	// 	frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	// }

	frame = h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	//find the highest event for each validator under each head
	candidateEvents := make(map[idx.ValidatorID][]hash.Event)
	// start := time.Now()
	for _, head := range allHeads {
		stakeCtr := h.Validators.NewCounter()
		var parents hash.Events
		parents.Add(head)
		for len(parents) > 0 {
			hE := parents[len(parents)-1]
			e := h.Dagi.GetEvent(hE)
			c := e.Creator()
			stakeCtr.Count(c)
			candidateEvents[c] = append(candidateEvents[c], hE)
			parents = parents[:len(parents)-1]
			for _, p := range e.Parents() {
				if h.Dagi.GetEvent(p).Frame() >= frame {
					// only add parents events from frame of self (events from prior frame cannot be ahead of self)
					parents.Add(p)
				}
			}

			if stakeCtr.Sum() >= h.Validators.TotalWeight() {
				// we have found an event for each validator under this head
				break
			}
		}
	}
	// elapsed := time.Since(start)
	// fmt.Println("Find events took ", elapsed)
	// from the highest event from each head, get the highest across all heads
	highestEvents := make(map[idx.ValidatorID]hash.Event)
	// start = time.Now()
	for validator, events := range candidateEvents {
		var maxSeq idx.Event
		maxSeq = 0
		for _, e := range events {
			seq := h.Dagi.GetEvent(e).Seq()
			if seq > maxSeq {
				highestEvents[validator] = e
				maxSeq = seq
			}
		}
	}
	// elapsed = time.Since(start)
	// fmt.Println("Highest events took ", elapsed)

	// start = time.Now()
	kGreaterCount := 0.0
	kGreaterStake := h.Validators.NewCounter()
	kPrev := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, nil, online) // calculate k for new event under consideration
	for _, e := range highestEvents {
		de := h.Dagi.GetEvent(e)
		eFrame := de.Frame()
		if eFrame > frame {
			// if the highest event of another validator is ahead in frame number it is ahead of self (no need to calculate k)
			kGreaterCount++
			kGreaterStake.Count(de.Creator())
		} else if eFrame == frame {
			k := h.EventRootKnowledgeByCountOnline(frame, e, nil, online)
			if k >= kPrev {
				kGreaterCount++
				kGreaterStake.Count(de.Creator())
			}
		}
	}
	// elapsed = time.Since(start)
	// fmt.Println("Count events took ", elapsed)
	n := float64(h.Validators.Len())

	// *** VARIABLE TIME INTERVAL***
	// medianT := timingMedianMean(h.TimingStats)
	// timePropConst = medianT
	// deltRealTime := passedTime / timePropConst

	// orderedStake := h.Validators.SortedWeights()
	// selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
	// selfStake := float64(h.Validators.GetWeightByIdx(h.Validators.GetIdx(selfID)))
	// threshParam = threshParam * float64(h.Validators.TotalWeight()) / float64(selfStake) / n
	if kGreaterCount/n*passedTime > threshParam {
		return true
	}
	return false
}

func (h *QuorumIndexer) ParentDifference(chosenHeads hash.Events) bool {
	var frame idx.Frame = 0
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}
	differentCount := 0
	for i, head1 := range chosenHeads {
		K1 := h.EventRootKnowledgeMatrix(head1, frame)
		for _, head2 := range chosenHeads[i+1:] {
			K2 := h.EventRootKnowledgeMatrix(head2, frame)
			_, kDiffCount, _ := h.RootKnowledgeDifference(K1, K2)
			if kDiffCount != 0 {
				differentCount++
			} else {
				allZero := true
				for _, rootCounter := range K1 {
					if rootCounter.Sum() != 0 {
						allZero = false
						break
					}
				}
				if allZero {
					differentCount++
				} else {
					break
				}
			}

		}
	}

	if differentCount >= 3 {
		return true
	}
	return false
}

func (h *QuorumIndexer) ValidatorComparisonOLD(allHeads hash.Events, online map[idx.ValidatorID]bool, threshParam float64) (float64, bool) {
	// find max frame of all heads
	var frame idx.Frame
	// for _, head := range allHeads {
	// 	frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	// }

	frame = h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	//find the highest event for each validator
	maxSeq := make(map[idx.ValidatorID]idx.Event)
	maxHead := make(map[idx.ValidatorID]hash.Event)
	for _, validator := range h.Validators.Idxs() {
		maxSeq[idx.ValidatorID(validator)] = 0
	}

	// first find which head has the highest sequence number for each validator
	for _, head := range allHeads {
		HBSeq := h.Dagi.GetMergedHighestBefore(head)
		for _, validator := range h.Validators.Idxs() {
			valID := h.Validators.GetID(validator)
			valSeq := HBSeq.Get(validator).Seq()
			if valSeq > maxSeq[valID] {
				maxSeq[valID] = valSeq
				maxHead[valID] = head
			}
		}
	}
	// Now find the events corresponding to the highest sequence numbers found above
	highestEvents := make(map[idx.ValidatorID]hash.Event)
	for valID, head := range maxHead {
		var parents hash.Events
		parents.Add(head)
		//breadth first search through head's subgraph to find the highest event for the current validator
		for len(parents) > 0 {
			eHash := parents[0]
			e := h.Dagi.GetEvent(eHash)
			c := e.Creator()
			if c == valID && e.Seq() == maxSeq[valID] {
				// this is the highest event
				highestEvents[valID] = eHash
				break
			}
			parents = parents[1:]       // remove event that was checked
			parents.Add(e.Parents()...) // add the event's parents to list of events to check
		}
	}

	kGreaterCount := 0.0
	kGreaterStake := h.Validators.NewCounter()
	kPrev := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, nil, online) // calculate k for new event under consideration
	// kPrev := h.EventRootKnowledgeMatrix(h.SelfParentEvent, frame)
	// kDAG := h.DAGRootKnowledgeMatrix(allHeads, frame)
	// _, kPrevDiffCount, _ := h.RootKnowledgeDifference(kDAG, kPrev)
	// _, _, kPrevDiffStake := h.RootKnowledgeDifference(kDAG, kPrev)
	for _, e := range highestEvents {

		if h.Dagi.GetEvent(e).Creator() != h.Dagi.GetEvent(h.SelfParentEvent).Creator() { //don't count self
			k := h.EventRootKnowledgeByCountOnline(frame, e, nil, online)
			// k := h.EventRootKnowledgeMatrix(e, frame)
			// _, kDiffCount, _ := h.RootKnowledgeDifference(kDAG, k)
			// _, _, kDiffStake := h.RootKnowledgeDifference(kDAG, k)
			if k >= kPrev {
				// if kPrevDiffCount >= kDiffCount {
				// if kPrevDiffStake >= kDiffStake {
				kGreaterCount++
				de := h.Dagi.GetEvent(e)
				kGreaterStake.Count(de.Creator())
			}
		}
	}
	n := float64(h.Validators.Len())
	selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
	// selfStake := float64(h.Validators.GetWeightByIdx(h.Validators.GetIdx(selfID)))
	// meanStake := float64(h.Validators.TotalWeight()) / n
	// threshParam = 1.0 - selfStake/meanStake*threshParam

	sortedIDs := h.Validators.SortedIDs()
	quorumCounter := h.Validators.NewCounter()
	numQuorum := 0
	quorumCounter.Count(selfID) // assume self is a properly functioning validator, count self plus minimum number of other validators for quourm
	for _, ID := range sortedIDs {
		if ID != selfID {
			quorumCounter.Count(ID)
			numQuorum++
		}
		if quorumCounter.HasQuorum() {
			break
		}

	}

	threshMax := float64(numQuorum) / n //Count based maximum threshold is minimum number of validators to reach quorum
	threshMin := 1.0 / n                //Count based minimum threshold is one validator

	// threshMax := float64(h.Validators.Quorum()-selfStake) // Stake based maximum threshold
	// sortedWeights := h.Validators.SortedWeights()
	// threshMin := float64(sortedWeights[len(sortedWeights)-1]) // Stake based minimum threshold
	if threshParam > threshMax {
		threshParam = threshMax
	}
	if threshParam < threshMin {
		threshParam = threshMin
	}

	// thresholdStake := threshParam * float64(h.Validators.TotalWeight())
	// stake := float64(kGreaterStake.Sum())
	// return kGreaterCount / n, stake >= thresholdStake

	thresholdCount := threshParam * float64(h.Validators.Len())
	return kGreaterCount / n, kGreaterCount >= thresholdCount

	// thresholdCount := threshParam * float64(h.Validators.Len())
	// thresholdStake := threshParam * float64(h.Validators.TotalWeight())
	// stake := float64(kGreaterStake.Sum())

	// return kGreaterCount / n, kGreaterCount >= thresholdCount && stake >= thresholdStake

}

func (h *QuorumIndexer) PiecewiseLinearCondition(chosenHeads hash.Events, online map[idx.ValidatorID]bool) (float64, float64) {
	newFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		newFrame = maxFrame(newFrame, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeByCountOnline(newFrame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	kWorld := 0.0
	for _, head := range chosenHeads {
		k := h.EventRootKnowledgeByCountOnline(newFrame, head, nil, online) // calculate k for most recent self Event
		if k > kWorld {
			kWorld = k
		}
	}
	// kPrev := h.EventRootKnowledgeByCountOnline(newFrame, h.SelfParentEvent, nil, online) // calculate k for most recent self event
	return kNew, kWorld
}

func (h *QuorumIndexer) LogisticTimeComparison(timePropConst float64, passedTime float64, chosenHeads hash.Events, nParents int, online map[idx.ValidatorID]bool) (float64, bool) {
	prevFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	newFrame := prevFrame

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		newFrame = maxFrame(newFrame, h.Dagi.GetEvent(head).Frame())
	}

	n := 0.0
	for _, isOnline := range online {
		if isOnline {
			n++
		}
	}

	// +++TODO there is an assumption that online nodes for prev are the same for new and this may not be correct, use cached value?
	kMin := 1.0 / (n * n)
	kNew := h.EventRootKnowledgeByCountOnline(newFrame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	// if kNew == 1.0 {
	// k = 1.0 occurs at infinity in the sigmoid
	// 	kNew -= kMin
	// }
	kPrev := h.EventRootKnowledgeByCountOnline(prevFrame, h.SelfParentEvent, nil, online) // calculate k for most recent self event
	kHead := 0.0
	for _, head := range chosenHeads {
		k := h.EventRootKnowledgeByCountOnline(newFrame, head, nil, online)
		// kPrev += kHead
		if k > kHead {
			kHead = k
		}
	}
	// prevFrame = newFrame
	// kPrev = kPrev / float64(len(chosenHeads))

	p := float64(nParents)

	tNew := -(1.0 / math.Log(p)) * math.Log(1.0/kNew-1.0)
	tPrev := -(1.0 / math.Log(p)) * math.Log(1.0/kPrev-1.0)
	tMin := -(1.0 / math.Log(p)) * math.Log(1.0/kMin-1.0)
	// tHead := -(1.0 / math.Log(p)) * math.Log(1.0/kHead-1.0)

	delt_k := 0.0
	if prevFrame < newFrame {
		// delt_k = tHead - tMin
		delt_k = tNew - tMin
		// Calculate remaining DAG time for previous event to complete its frame
		roots := h.lachesis.Store.GetFrameRoots(prevFrame + 1)
		kRootMin := 1.0 - kMin
		for _, root := range roots {
			kRoot := h.EventRootKnowledgeByCountOnline(prevFrame, root.ID, nil, online)
			if kRoot < kRootMin {
				kRootMin = kRoot
			}
		}
		tRoot := -(1.0 / math.Log(p)) * math.Log(1.0/kRootMin-1.0)
		if tRoot > tPrev {
			delt_k += tRoot - tPrev
		}

		//if previous is more than one frame behind, calulate DAG time duration of those intervening frames

		for frame := prevFrame + 2; frame < newFrame; frame++ {
			kRootMin = 1.0 - kMin
			roots := h.lachesis.Store.GetFrameRoots(frame)
			for _, root := range roots {
				kRoot := h.EventRootKnowledgeByCountOnline(frame, root.ID, nil, online)
				if kRoot < kRootMin {
					kRootMin = kRoot
				}
			}
			tRoot := -(1.0 / math.Log(p)) * math.Log(1.0/kRootMin-1.0)
			delt_k += tRoot - tMin
		}
	} else {
		delt_k = tNew - tPrev
		// delt_k = tHead - tPrev

	}

	// *** VARIABLE TIME INTERVAL***
	// medianT := timingMedianMean(h.TimingStats)
	// timePropConst = medianT
	// deltRealTime := passedTime / timePropConst
	// meanDelt := math.Sqrt(delt_k * deltRealTime) //geometric mean

	// dt := 1.0
	if delt_k >= 1.5 {
		// fmt.Print(", ", kNew)
		return delt_k, true
		// return kNew, true
	}
	return delt_k, false
	// return kNew, false
}

func (h *QuorumIndexer) LogisticTimingConditionByCountOnlineAndTime(timePropConst float64, passedTime float64, chosenHeads hash.Events, nParents int, online map[idx.ValidatorID]bool) (float64, bool) {
	prevFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	newFrame := prevFrame

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		newFrame = maxFrame(newFrame, h.Dagi.GetEvent(head).Frame())
	}

	n := 0.0
	for _, isOnline := range online {
		if isOnline {
			n++
		}
	}

	// +++TODO there is an assumption that online nodes for prev are the same for new and this may not be correct, use cached value?
	kMin := 1.0 / (n * n)
	kNew := h.EventRootKnowledgeByCountOnline(newFrame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	// if kNew == 1.0 {
	// k = 1.0 occurs at infinity in the sigmoid
	// 	kNew -= kMin
	// }
	kPrev := h.EventRootKnowledgeByCountOnline(prevFrame, h.SelfParentEvent, nil, online) // calculate k for most recent self event
	// kPrev := 0.0
	// for _, head := range chosenHeads {
	// 	kHead := h.EventRootKnowledgeByCountOnline(newFrame, head, nil, online)
	// 	// kPrev += kHead
	// 	if kHead > kPrev {
	// 		kPrev = kHead
	// 	}
	// }
	// prevFrame = newFrame
	// kPrev = kPrev / float64(len(chosenHeads))

	p := float64(nParents)

	tNew := -(1.0 / math.Log(p)) * math.Log(1.0/kNew-1.0)
	tPrev := -(1.0 / math.Log(p)) * math.Log(1.0/kPrev-1.0)
	tMin := -(1.0 / math.Log(p)) * math.Log(1.0/kMin-1.0)

	delt_k := 0.0
	if prevFrame < newFrame {
		delt_k = tNew - tMin
		// Calculate remaining DAG time for previous event to complete its frame
		roots := h.lachesis.Store.GetFrameRoots(prevFrame + 1)
		kRootMin := 1.0 - kMin
		for _, root := range roots {
			kRoot := h.EventRootKnowledgeByCountOnline(prevFrame, root.ID, nil, online)
			if kRoot < kRootMin {
				kRootMin = kRoot
			}
		}
		tRoot := -(1.0 / math.Log(p)) * math.Log(1.0/kRootMin-1.0)
		if tRoot > tPrev {
			delt_k += tRoot - tPrev
		}

		//if previous is more than one frame behind, calulate DAG time duration of those intervening frames

		for frame := prevFrame + 2; frame < newFrame; frame++ {
			kRootMin = 1.0 - kMin
			roots := h.lachesis.Store.GetFrameRoots(frame)
			for _, root := range roots {
				kRoot := h.EventRootKnowledgeByCountOnline(frame, root.ID, nil, online)
				if kRoot < kRootMin {
					kRootMin = kRoot
				}
			}
			tRoot := -(1.0 / math.Log(p)) * math.Log(1.0/kRootMin-1.0)
			delt_k += tRoot - tMin
		}
	} else {
		delt_k = tNew - tPrev

	}

	// *** VARIABLE TIME INTERVAL***
	// medianT := timingMedianMean(h.TimingStats)
	// timePropConst = medianT
	deltRealTime := passedTime / timePropConst
	meanDelt := math.Sqrt(delt_k * deltRealTime) //geometric mean

	dt := 1.0
	if meanDelt >= dt {
		// fmt.Print(", ", kNew)
		// return kNew, true
		return delt_k, true
	}

	return delt_k, false
	// return kNew, true
}

func (h *QuorumIndexer) LogisticTimingConditionByCountAndTime(passedTime float64, chosenHeads hash.Events, nParents int) (float64, bool) {

	// timePropConst := 1 / 110.0
	medianT := timingMedianMean(h.TimingStats)
	timePropConst := 1.0 / medianT

	tMax := 2 * math.Log(float64(h.Validators.Quorum())) / math.Log(float64(nParents))
	framePrev := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	frameNew := framePrev
	// find if a head's frame is ahead of prev self event's frame
	for _, head := range chosenHeads {
		frameNew = maxFrame(frameNew, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeQByCount(frameNew, h.SelfParentEvent, chosenHeads) // calculate k for new event under consideration
	tNew := 0.0
	if kNew < 1.0 {
		tNew = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kNew-1.0)
	} else {
		tNew = tMax
	}

	kPrev := h.EventRootKnowledgeQByCount(framePrev, h.SelfParentEvent, nil) // calculate k for most recent self event

	tPrev := 0.0
	delt_k := 0.0
	if framePrev == frameNew {
		tPrev = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	} else {
		kMin := 1.0 / (float64(h.Validators.Quorum()) * float64(h.Validators.Quorum()))
		tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)
		kPrev = h.EventRootKnowledgeQByCount(framePrev, h.SelfParentEvent, nil) // calculate k for most recent self event
		tPrev = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
		diffFrames := frameNew - framePrev // number of frames prev is behind new
		tFrame := tMax - tMin              // duration of an entire frame
		tPrev = tPrev - tFrame*float64(diffFrames)
	}
	delt_k = tNew - tPrev

	deltRealTime := passedTime * timePropConst
	meanDelt := math.Sqrt(delt_k * deltRealTime) //geometric mean
	stakeEventRate := 1.0
	if meanDelt >= stakeEventRate {
		// fmt.Print(", ", kNew)
		return kNew, true
	}

	return kNew, false
}

func (h *QuorumIndexer) EventRootKnowledgeMatrix(event hash.Event, frame idx.Frame) map[election.RootAndSlot]*pos.WeightCounter {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	RootKnowledge := make(map[election.RootAndSlot]*pos.WeightCounter, len(roots))
	for _, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, nil) //compute for new event
		RootKnowledge[root] = FCProgress[0]
	}
	return RootKnowledge
}

func (h *QuorumIndexer) DAGRootKnowledgeMatrix(heads hash.Events, frame idx.Frame) map[election.RootAndSlot]*pos.WeightCounter {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	RootKnowledge := make(map[election.RootAndSlot]*pos.WeightCounter, len(roots))
	for _, root := range roots {
		RootKnowledge[root] = h.Validators.NewCounter()
		for _, head := range heads {
			FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(head, root.ID, nil, nil) //compute for new event
			for _, validator := range h.Validators.Idxs() {
				if FCProgress[0].IsCounted(validator) {
					RootKnowledge[root].CountByIdx(validator)
				}
			}
		}
	}
	return RootKnowledge
}

func (h *QuorumIndexer) RootKnowledgeDifference(K1, K2 map[election.RootAndSlot]*pos.WeightCounter) (map[election.RootAndSlot]*pos.WeightCounter, int, pos.Weight) {
	// Get root knowledge of K1 that is NOT known in K2
	countDifference := 0
	var stakeDifference pos.Weight = 0
	RootKnowledgeDifference := make(map[election.RootAndSlot]*pos.WeightCounter, len(K1))
	for root, rK1 := range K1 {
		RootKnowledgeDifference[root] = h.Validators.NewCounter()
		rK2 := K2[root]
		for _, validator := range h.Validators.Idxs() {
			if rK1.IsCounted(validator) && !rK2.IsCounted(validator) {
				RootKnowledgeDifference[root].CountByIdx(validator)
				countDifference++
			}
		}
		rootValidatorIdx := h.Validators.GetIdx(root.Slot.Validator)
		rootStake := h.Validators.GetWeightByIdx(rootValidatorIdx)
		stakeDifference += rootStake * RootKnowledgeDifference[root].Sum()
	}
	return RootKnowledgeDifference, countDifference, stakeDifference
}

func (h *QuorumIndexer) UpdateTimingStats(value float64, source idx.ValidatorID) {
	expMov := h.TimingStats[source]
	if h.TimingStats[source].mean == 0.0 {
		expMov.mean = value
	} else {
		expMov.mean = expMov.expCoeff*value + (1.0-expMov.expCoeff)*expMov.mean
	}
	deviation := value - expMov.mean
	if expMov.variance == 0.0 {
		expMov.variance = deviation * deviation
	} else {
		expMov.variance = expMov.expCoeff*deviation*deviation + (1.0-expMov.expCoeff)*expMov.variance
	}
	h.TimingStats[source] = expMov
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
	for validatorIdx := idx.Validator(0); validatorIdx < h.Validators.Len(); validatorIdx++ {

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
	for validatorIdx := idx.Validator(0); validatorIdx < h.Validators.Len(); validatorIdx++ {
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
