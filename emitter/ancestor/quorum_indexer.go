package ancestor

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/Fantom-foundation/go-opera/utils/piecefunc"
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
	validators *pos.Validators

	SelfParentEvent     hash.Event
	SelfParentEventFlag bool

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
		TimingStats:         *newTimingStats(validators),
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
		rootProgressMetrics[i].NewRootKnowledge = h.eventRootKnowledgeQByStake(maxHeadFrame, h.SelfParentEvent, heads)
	}
	return rootProgressMetrics
}

// func (h *QuorumIndexer) GetMetricsOfRootProgress(heads hash.Events, chosenHeads hash.Events) []RootProgressMetrics {
// 	// This function is indended to be used in the process of
// 	// selecting event block parents from a set of head options.
// 	// This function returns useful metrics for assessing
// 	// how much a validator will progress toward producing a root when using head as a parent.
// 	// creator denotes the validator creating a new event block.
// 	// chosenHeads are heads that have already been selected
// 	// head denotes the event block of another validator that is being considered as a potential parent.

// 	// find max frame number of self event block, and chosen heads
// 	currentFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

// 	// find frame number of each head, and max frame number
// 	var maxHeadFrame idx.Frame = currentFrame
// 	headFrame := make([]idx.Frame, len(heads))

// 	for i, head := range heads {
// 		headFrame[i] = h.Dagi.GetEvent(head).Frame()
// 		if headFrame[i] > maxHeadFrame {
// 			maxHeadFrame = headFrame[i]
// 		}
// 	}

// 	for _, head := range chosenHeads {
// 		if h.Dagi.GetEvent(head).Frame() > maxHeadFrame {
// 			maxHeadFrame = h.Dagi.GetEvent(head).Frame()
// 		}
// 	}

// 	// only retain heads with max frame number
// 	var rootProgressMetrics []RootProgressMetrics
// 	var maxHeads hash.Events
// 	for i, head := range heads {
// 		if headFrame[i] >= maxHeadFrame {
// 			rootProgressMetrics = append(rootProgressMetrics, h.newRootProgressMetrics(i))
// 			maxHeads = append(maxHeads, head)
// 		}
// 	}

// 	maxFrameRoots := h.lachesis.Store.GetFrameRoots(maxHeadFrame)
// 	CurrentRootKnowledge := make([]KIdx, len(maxFrameRoots))
// 	HeadsRootKnowledge := make([]sortedKIdx, len(maxHeads))
// 	for i, _ := range HeadsRootKnowledge {
// 		HeadsRootKnowledge[i] = make([]KIdx, len(maxFrameRoots))
// 	}
// 	for j, root := range maxFrameRoots {
// 		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, maxHeads, chosenHeads)
// 		currentFCProgress := FCProgress[len(FCProgress)-1]

// 		if currentFCProgress.Sum() <= h.validators.Quorum() {
// 			CurrentRootKnowledge[j].K = float64(currentFCProgress.Sum())
// 		} else {
// 			CurrentRootKnowledge[j].K = float64(h.validators.Quorum())
// 		}
// 		CurrentRootKnowledge[j].Root = root

// 		for i, _ := range maxHeads {
// 			// Below metrics are computed in order of importance (most important first)
// 			if FCProgress[i].HasQuorum() && !currentFCProgress.HasQuorum() {
// 				// This means the root forkless causes the creator when head is a parent, but does not forkless cause without the head
// 				rootProgressMetrics[i].NewFCWeight.Count(root.Slot.Validator)
// 			}

// 			if FCProgress[i].Sum() <= h.validators.Quorum() {
// 				HeadsRootKnowledge[i][j].K = float64(FCProgress[i].Sum())
// 			} else {
// 				HeadsRootKnowledge[i][j].K = float64(h.validators.Quorum())
// 			}
// 			HeadsRootKnowledge[i][j].Root = root

// 			if FCProgress[i].Sum() > 0 && currentFCProgress.Sum() == 0 {
// 				// this means that creator with head parent observes the root, but creator on its own does not
// 				// i.e. this is a new root observed via the head
// 				rootProgressMetrics[i].NewObservedRootWeight.Count(root.Slot.Validator)
// 			}

// 		}
// 	}

// 	sort.Sort(sortedKIdx(CurrentRootKnowledge))
// 	var currentKnowledge uint64 = 0
// 	var bestRootsStake pos.Weight = 0
// 	for _, kidx := range CurrentRootKnowledge {
// 		rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
// 		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
// 		if bestRootsStake >= h.validators.Quorum() {
// 			break
// 		} else if bestRootsStake+rootStake <= h.validators.Quorum() {
// 			currentKnowledge += uint64(kidx.K) * uint64(rootStake)
// 			bestRootsStake += rootStake
// 		} else {
// 			partialStake := h.validators.Quorum() - bestRootsStake
// 			currentKnowledge += uint64(kidx.K) * uint64(partialStake)
// 			bestRootsStake += partialStake // this should trigger the break condition above
// 		}
// 	}
// 	for i, _ := range maxHeads {
// 		sort.Sort(HeadsRootKnowledge[i])
// 		rootProgressMetrics[i].NewRootKnowledge = 0
// 		var bestRootsStake pos.Weight = 0
// 		for _, kidx := range HeadsRootKnowledge[i] {
// 			rootValidatorIdx := h.validators.GetIdx(kidx.Root.Slot.Validator)
// 			rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
// 			if bestRootsStake >= h.validators.Quorum() {
// 				break
// 			} else if bestRootsStake+rootStake <= h.validators.Quorum() {
// 				rootProgressMetrics[i].NewRootKnowledge += uint64(kidx.K) * uint64(rootStake)
// 				bestRootsStake += rootStake
// 			} else {
// 				partialStake := h.validators.Quorum() - bestRootsStake
// 				rootProgressMetrics[i].NewRootKnowledge += uint64(kidx.K) * uint64(partialStake)
// 				bestRootsStake += partialStake // this should trigger the break condition above
// 			}
// 		}
// 		rootProgressMetrics[i].NewRootKnowledge -= currentKnowledge
// 	}
// 	return rootProgressMetrics
// }

// func (h *QuorumIndexer) GetMetricsOfRootProgressOLD(heads hash.Events, chosenHeads hash.Events) []RootProgressMetrics {
// 	// This function is indended to be used in the process of
// 	// selecting event block parents from a set of head options.
// 	// This function returns useful metrics for assessing
// 	// how much a validator will progress toward producing a root when using head as a parent.
// 	// creator denotes the validator creating a new event block.
// 	// chosenHeads are heads that have already been selected
// 	// head denotes the event block of another validator that is being considered as a potential parent.

// 	// find max frame number of self event block, and chosen heads
// 	currentFrame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

// 	// find frame number of each head, and max frame number
// 	var maxHeadFrame idx.Frame = currentFrame
// 	headFrame := make([]idx.Frame, len(heads))

// 	for i, head := range heads {
// 		headFrame[i] = h.Dagi.GetEvent(head).Frame()
// 		if headFrame[i] > maxHeadFrame {
// 			maxHeadFrame = headFrame[i]
// 		}
// 	}

// 	for _, head := range chosenHeads {
// 		if h.Dagi.GetEvent(head).Frame() > maxHeadFrame {
// 			maxHeadFrame = h.Dagi.GetEvent(head).Frame()
// 		}
// 	}

// 	// only retain heads with max frame number
// 	var rootProgressMetrics []RootProgressMetrics
// 	var maxHeads hash.Events
// 	for i, head := range heads {
// 		if headFrame[i] >= maxHeadFrame {
// 			rootProgressMetrics = append(rootProgressMetrics, h.newRootProgressMetrics(i))
// 			maxHeads = append(maxHeads, head)
// 		}
// 	}
// 	// +++ToDo only retain chosenHeads with max frame number

// 	maxFrameRoots := h.lachesis.Store.GetFrameRoots(maxHeadFrame)
// 	//+++todo, does Store.GetFrameRoots return roots for an undecided frame (or only for decided frames)? If no, need to get roots elsewhere
// 	for _, root := range maxFrameRoots {
// 		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, maxHeads, chosenHeads)

// 		currentFCProgress := FCProgress[len(FCProgress)-1]
// 		for i, _ := range maxHeads {
// 			// Below metrics are computed in order of importance (most important first)
// 			if FCProgress[i].HasQuorum() && !currentFCProgress.HasQuorum() {
// 				// This means the root forkless causes the creator when head is a parent, but does not forkless cause without the head
// 				rootProgressMetrics[i].NewFCWeight.Count(root.Slot.Validator)
// 			}

// 			if !FCProgress[i].HasQuorum() {
// 				// if the root does not forkless cause even with the head, add improvement head makes toward forkless cause
// 				rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
// 				rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
// 				rootProgressMetrics[i].NewRootKnowledge += uint64(rootStake) * uint64(FCProgress[i].Sum()-currentFCProgress.Sum())

// 				// rootProgressMetrics[i].NewRootKnowledge += FCProgress[i].Sum() - currentFCProgress.Sum()

// 				if FCProgress[i].Sum() > 0 && currentFCProgress.Sum() == 0 {
// 					// this means that creator with head parent observes the root, but creator on its own does not
// 					// i.e. this is a new root observed via the head
// 					rootProgressMetrics[i].NewObservedRootWeight.Count(root.Slot.Validator)
// 				}
// 			}
// 		}
// 	}
// 	return rootProgressMetrics
// }

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

func (h *QuorumIndexer) EventRootKnowledgeByCountOnline(frame idx.Frame, event hash.Event, chosenHeads hash.Events, online map[idx.ValidatorID]bool) float64 {
	roots := h.lachesis.Store.GetFrameRoots(frame)

	// find total stake of online nodes and ensure it meets quourum
	wOnline := h.validators.NewCounter()
	numOnline := 0
	for ID, isOnline := range online {
		if isOnline {
			numOnline++
			wOnline.Count(ID)
		}
	}

	// if less than quorum are online, add the minimum number of nodes (i.e. largest offline nodes) that need to come online for quourm to be online
	// if !wOnline.HasQuorum() {
	// 	sortedWeights := h.validators.SortedWeights()
	// 	sortedIDs := h.validators.SortedIDs()
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

	weights := h.validators.SortedWeights()
	ids := h.validators.SortedIDs()

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
			kNew += kidx.K
			bestRootsStake = h.validators.Quorum() // this will trigger the break condition above
			numRootsForQ++
			rootValidators = append(rootValidators, rootValidatorIdx)
		}
	}

	// calculate how many extra roots are needed for quorum (if any), to get the denominator of k
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
	return kNew / numRootsForQ // this result should be less than or equal to 1
}

func (h *QuorumIndexer) eventRootKnowledgeQByStake(frame idx.Frame, event hash.Event, chosenHeads hash.Events) float64 {
	roots := h.lachesis.Store.GetFrameRoots(frame)
	Q := float64(h.validators.Quorum())
	D := (Q * Q)

	// calculate k for event under consideration

	RootKnowledge := make([]KIdx, len(roots))
	for i, root := range roots {
		FCProgress := h.lachesis.DagIndex.ForklessCauseProgress(event, root.ID, nil, chosenHeads) //compute for new event
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
			bestRootsStake = h.validators.Quorum() // this will trigger the break condition above
		}
	}
	kNew = kNew / D

	return kNew
}

func (h *QuorumIndexer) LogisticTimingDeltat(chosenHeads hash.Events, nParents int, receivedStake pos.Weight) float64 {

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeQByCount(frame, h.SelfParentEvent, chosenHeads) // calculate k for new event under consideration
	kPrev := h.EventRootKnowledgeQByCount(frame, h.SelfParentEvent, nil)        // calculate k for most recent self event

	tPrev := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	tNew := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kNew-1.0)
	Deltat := tNew - tPrev

	kMin := 1.0 / (float64(h.validators.Quorum()) * float64(h.validators.Quorum()))
	tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)
	tMax := 2 * math.Log(float64(h.validators.Quorum())) / math.Log(float64(nParents))
	if kNew == 1 {
		// fmt.Print("Deltat inf")
		Deltat = tMax - tPrev
	}
	if kPrev == 0 {
		// +++TODO loop until finding a past frame with an event. Prev could be very old!

		kPrev := h.EventRootKnowledgeQByCount(frame-1, h.SelfParentEvent, nil) // calculate k for most recent self event

		tPrev = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)

		Deltat = (tNew - tMin) + (tMax - tPrev)
	}
	return Deltat
}

func (h *QuorumIndexer) LogisticTimingConditionByCountOnline(chosenHeads hash.Events, nParents int, online map[idx.ValidatorID]bool) (float64, bool) {

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	// +++TODO there is an assumption that online nodes for prev are the same for new and this may not be correct, use cached value?
	kNew := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	kPrev := h.EventRootKnowledgeByCountOnline(frame, h.SelfParentEvent, nil, online)        // calculate k for most recent self event
	kCond := 0.0
	// if kPrev == 0 {
	// 	// +++TODO loop until finding a past frame with an event. Prev could be very old!
	// 	kPrev = h.EventRootKnowledgeQByCount(frame-1, h.SelfParentEvent, nil) // calculate k for most recent self event
	// 	tPrev := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	// 	tMax := 2 * math.Log(float64(h.validators.Quorum())) / math.Log(float64(nParents))

	// 	kMin := 1.0 / (float64(h.validators.Quorum()) * float64(h.validators.Quorum()))
	// 	tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)

	// 	tCond := tMin - (tMax - tPrev) + 1.0
	// 	kCond = 1.0 / (1.0 + math.Exp(-tCond*math.Log(float64(nParents))))
	// } else {
	meanInvWeight := 0.0 //+++TODO do this for online nodes only?
	meanWeight := 0.0
	var maxWeight pos.Weight = 0
	for _, idx := range h.validators.Idxs() {
		meanInvWeight += 1 / float64(h.validators.GetWeightByIdx(idx))
		meanWeight += float64(h.validators.GetWeightByIdx(idx))
		if h.validators.GetWeightByIdx(idx) > maxWeight {
			maxWeight = h.validators.GetWeightByIdx(idx)
		}
	}
	meanWeight = meanWeight / float64(h.validators.Len())
	meanInvWeight = meanInvWeight / float64(h.validators.Len())

	selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
	selfIdx := h.validators.GetIdx(selfID)
	selfWeight := h.validators.GetWeightByIdx(selfIdx)
	// dt := 1.0 * (1 / meanInvWeight) * (1 / float64(selfWeight))
	dt := 1.0 * meanWeight * (1 / float64(selfWeight))
	kCond = math.Pow(float64(nParents), dt) * kPrev / (math.Pow(float64(nParents), dt)*kPrev - kPrev + 1.0) // This condition is based on logistic growth
	// }

	if kNew >= kCond {
		// fmt.Print(", ", kNew)
		return kNew, true
	}

	return kNew, false
}

func (h *QuorumIndexer) LogisticTimingConditionByCount(chosenHeads hash.Events, nParents int, receivedStake pos.Weight) (float64, bool) {

	frame := h.Dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		frame = maxFrame(frame, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeQByCount(frame, h.SelfParentEvent, chosenHeads) // calculate k for new event under consideration
	kPrev := h.EventRootKnowledgeQByCount(frame, h.SelfParentEvent, nil)        // calculate k for most recent self event
	kCond := 0.0
	// if kPrev == 0 {
	// 	// +++TODO loop until finding a past frame with an event. Prev could be very old!
	// 	kPrev = h.EventRootKnowledgeQByCount(frame-1, h.SelfParentEvent, nil) // calculate k for most recent self event
	// 	tPrev := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	// 	tMax := 2 * math.Log(float64(h.validators.Quorum())) / math.Log(float64(nParents))

	// 	kMin := 1.0 / (float64(h.validators.Quorum()) * float64(h.validators.Quorum()))
	// 	tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)

	// 	tCond := tMin - (tMax - tPrev) + 1.0
	// 	kCond = 1.0 / (1.0 + math.Exp(-tCond*math.Log(float64(nParents))))
	// } else {
	kCond = float64(nParents) * kPrev / (float64(nParents)*kPrev - kPrev + 1.0) // This condition is based on logistic growth
	// }

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

func (h *QuorumIndexer) GetMetricOfLogistic(chosenHeads hash.Events, nParents int) Metric {
	// this function returns t_k, the logistic time difference between the current event being considered and the previous self event
	// t_k can be thought of as time difference in units of `DAG progress time'

	framePrev := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	frameNew := framePrev
	// find if a head's frame is ahead of prev self event's frame
	for _, head := range chosenHeads {
		frameNew = maxFrame(frameNew, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeQByCount(frameNew, h.SelfParentEvent, chosenHeads) // calculate k for new event under consideration
	tMax := 2 * math.Log(float64(h.validators.Quorum())) / math.Log(float64(nParents))
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
		kMin := 1.0 / (float64(h.validators.Quorum()) * float64(h.validators.Quorum()))
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

func (h *QuorumIndexer) LogisticTimingConditionByCountOnlineAndTime(passedTime float64, chosenHeads hash.Events, nParents int, online map[idx.ValidatorID]bool) (float64, bool) {

	// *** UNIFORM TIME INTERVAL***
	timePropConst := 1 / 90.0

	// ***  STAKE DEPENDENT TIME INTERVAL***
	// selfID := h.Dagi.GetEvent(h.SelfParentEvent).Creator()
	// selfIdx := h.validators.GetIdx(selfID)
	// selfWeight := h.validators.GetWeightByIdx(selfIdx)

	// maxWeight := selfWeight
	// for _, idx := range h.validators.Idxs() {
	// 	weight := h.validators.GetWeightByIdx(idx)
	// 	if weight > maxWeight {
	// 		maxWeight = weight
	// 	}
	// }
	// timePropConst := (1 / 80.0) * (float64(selfWeight) / float64(maxWeight))

	// *** VARIABLE TIME INTERVAL***
	// medianT := timingMedianMean(h.TimingStats)
	// timePropConst := 1.0 / medianT

	tMax := 2 * math.Log(float64(h.validators.Quorum())) / math.Log(float64(nParents))
	framePrev := h.Dagi.GetEvent(h.SelfParentEvent).Frame()
	frameNew := framePrev
	// find if a head's frame is ahead of prev self event's frame
	for _, head := range chosenHeads {
		frameNew = maxFrame(frameNew, h.Dagi.GetEvent(head).Frame())
	}

	kNew := h.EventRootKnowledgeByCountOnline(frameNew, h.SelfParentEvent, chosenHeads, online) // calculate k for new event under consideration
	tNew := 0.0
	if kNew < 1.0 {
		tNew = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kNew-1.0)
	} else {
		tNew = tMax
	}

	kPrev := h.EventRootKnowledgeByCountOnline(framePrev, h.SelfParentEvent, nil, online) // calculate k for most recent self event

	tPrev := 0.0
	delt_k := 0.0
	if framePrev == frameNew {
		tPrev = -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kPrev-1.0)
	} else {
		kMin := 1.0 / (float64(h.validators.Quorum()) * float64(h.validators.Quorum()))
		tMin := -(1.0 / math.Log(float64(nParents))) * math.Log(1.0/kMin-1.0)
		kPrev = h.EventRootKnowledgeByCountOnline(framePrev, h.SelfParentEvent, nil, online) // calculate k for most recent self event
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

func (h *QuorumIndexer) LogisticTimingConditionByCountAndTime(passedTime float64, chosenHeads hash.Events, nParents int) (float64, bool) {

	// timePropConst := 1 / 110.0
	medianT := timingMedianMean(h.TimingStats)
	timePropConst := 1.0 / medianT

	tMax := 2 * math.Log(float64(h.validators.Quorum())) / math.Log(float64(nParents))
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
		kMin := 1.0 / (float64(h.validators.Quorum()) * float64(h.validators.Quorum()))
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
