package ancestor

import (
	"math"
	"sort"

	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/utils/wmedian"
)

type RootProgressMetrics struct {
	CreatorFrame             idx.Frame
	HeadFrame                idx.Frame
	FCRoots                  map[hash.Event]bool
	ForklessCauseProgressMap map[hash.Event]*pos.WeightCounter
}

type DagIndex interface {
	dagidx.VectorClock
}
type DiffMetricFn func(median, current, update idx.Event, validatorIdx idx.Validator) Metric

type QuorumIndexer struct {
	dagi       DagIndex
	validators *pos.Validators

	SelfParentEvent hash.Event

	lachesis *abft.Lachesis

	globalMatrix     Matrix
	selfParentSeqs   []idx.Event
	globalMedianSeqs []idx.Event
	dirty            bool
	searchStrategy   SearchStrategy

	diffMetricFn DiffMetricFn
}

func NewQuorumIndexer(validators *pos.Validators, dagi DagIndex, diffMetricFn DiffMetricFn, lachesis *abft.Lachesis) *QuorumIndexer {
	return &QuorumIndexer{
		globalMatrix:     NewMatrix(validators.Len(), validators.Len()),
		globalMedianSeqs: make([]idx.Event, validators.Len()),
		selfParentSeqs:   make([]idx.Event, validators.Len()),
		dagi:             dagi,
		validators:       validators,
		diffMetricFn:     diffMetricFn,
		dirty:            true,
		lachesis:         lachesis,
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
	vecClock := h.dagi.GetMergedHighestBefore(event.ID())
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

func (h *QuorumIndexer) GetMetricsOfRootProgress(head hash.Event) RootProgressMetrics {
	// This function is indended to be used in the process of
	// selecting event block parents from a set of head options.
	// This function returns useful metrics for assessing
	// how much a validator will progress toward producing a root when using head as a parent

	var rootProgressMetrics RootProgressMetrics

	// first check to see if head has produced a root by comparing its current frame with lastDecidedFrame
	// if head has produced a new root, then selecting it as a parent guarantees creator will produce a new root, if it has not already

	headFrame := h.dagi.GetEvent(head).Frame()
	rootProgressMetrics.HeadFrame = headFrame
	creatorFrame := h.dagi.GetEvent(h.SelfParentEvent).Frame()
	rootProgressMetrics.CreatorFrame = creatorFrame
	// next calculate which previous roots id observes in its subgraph
	// Lowest After is used to determine which validators know a root
	rootProgressMetrics.ForklessCauseProgressMap = make(map[hash.Event]*pos.WeightCounter, h.validators.Len())

	//+++QUESTION, does Store.GetFrameRoots return roots for an undecided frame (or only for decided frames)? If no, need to get roots elsewhere
	for _, it := range h.lachesis.Store.GetFrameRoots(headFrame) {
		// +++ TODO only loop over roots in head's subgraph?
		rootProgressMetrics.ForklessCauseProgressMap[it.ID] = h.lachesis.DagIndex.ForklessCauseProgress(head, it.ID)
		rootProgressMetrics.FCRoots[it.ID] = rootProgressMetrics.ForklessCauseProgressMap[it.ID].HasQuorum()
	}

	// return the following useful metrics
	// 1) rootProgressMetrics.CreatorFrame and .HeadFrame: To check if head's frame is ahead of creator frame
	// 2) rootProgressMetrics.FCRoots: Specifies which roots forkless cause head
	// 3) rootProgressMetrics.ForklessCauseProgressMap: Progress of each root in forkless causing head

	return rootProgressMetrics
}

func (h *QuorumIndexer) GetMetricOf(id hash.Event) Metric {
	if h.dirty {
		h.recacheState()
	}
	vecClock := h.dagi.GetMergedHighestBefore(id)
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
