package ancestor

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/utils/wmedian"
)

type sortedRootProgressMetrics []RootProgressMetrics

type RootProgressMetrics struct {
	idx int
	// head                  hash.Event
	// CreatorFrame          idx.Frame
	// HeadFrame             idx.Frame
	newObservedRootWeight pos.WeightCounter
	newFCWeight           pos.WeightCounter
	newRootKnowledge      pos.Weight
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
	dagi       DagIndex
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
		dagi:                dagi,
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

	if m[i].newFCWeight.Sum() != m[j].newFCWeight.Sum() {
		return m[i].newFCWeight.Sum() > m[j].newFCWeight.Sum()
	}

	if m[i].newRootKnowledge != m[j].newRootKnowledge {
		return m[i].newRootKnowledge > m[j].newRootKnowledge
	}

	if m[i].newObservedRootWeight.Sum() != m[j].newObservedRootWeight.Sum() {
		return m[i].newObservedRootWeight.Sum() > m[j].newObservedRootWeight.Sum()
	}
	return true
}

func (h *QuorumIndexer) newRootProgressMetrics(headIdx int) RootProgressMetrics {
	var metric RootProgressMetrics
	metric.newRootKnowledge = 0
	metric.idx = headIdx
	metric.newObservedRootWeight = *h.validators.NewCounter()
	metric.newFCWeight = *h.validators.NewCounter()
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
	currentFrame := h.dagi.GetEvent(h.SelfParentEvent).Frame()

	// find frame number of each head, and max frame number
	var maxHeadFrame idx.Frame = currentFrame
	headFrame := make([]idx.Frame, len(heads))

	for i, head := range heads {
		headFrame[i] = h.dagi.GetEvent(head).Frame()
		if headFrame[i] > maxHeadFrame {
			maxHeadFrame = headFrame[i]
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
				rootProgressMetrics[i].newFCWeight.Count(root.Slot.Validator)
			}

			if !FCProgress[i].HasQuorum() {
				// if the root does not forkless cause even with the head, add improvement head makes toward forkless cause
				rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
				rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
				rootProgressMetrics[i].newRootKnowledge += rootStake * (FCProgress[i].Sum() - currentFCProgress.Sum())

				// rootProgressMetrics[i].newRootKnowledge += FCProgress[i].Sum() - currentFCProgress.Sum()

				if FCProgress[i].Sum() > 0 && currentFCProgress.Sum() == 0 {
					// this means that creator with head parent observes the root, but creator on its own does not
					// i.e. this is a new root observed via the head
					rootProgressMetrics[i].newObservedRootWeight.Count(root.Slot.Validator)
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

func (h *QuorumIndexer) GetTimingMetric(chosenHeads hash.Events) Metric {
	// Returns a metric between [0,1) for how much an event block with given chosenHeads
	// will advance the DAG. This can be used in determining if an event block should be created,
	// or if the validator should wait until it can progress the DAG further via choosing better heads.

	var prevFramePrevEventRootKnowledge pos.Weight = 0
	var prevFrameNewEventRootKnowledge pos.Weight = 0

	var newFramePrevEventRootKnowledge pos.Weight = 0
	var newFrameNewEventRootKnowledge pos.Weight = 0

	var metric float64

	prevFrame := h.dagi.GetEvent(h.SelfParentEvent).Frame()
	newFrame := h.dagi.GetEvent(h.SelfParentEvent).Frame()

	// find max frame when parents are selected
	for _, head := range chosenHeads {
		newFrame = maxFrame(newFrame, h.dagi.GetEvent(head).Frame())
	}
	//note that if this new event block is a root and starts a new frame but none of its parents are from the new frame,
	//then it will not make any progress to producing a new root for the new frame, so we don't need to bother calculating that (zero) progress

	if prevFrame+1 < newFrame {
		// if the new event is more than one frame ahead of the previous event block's frame then return the maximum metric
		// this may occur if the node has been offline temporarily, or has been disconnected from the P2P network.
		return Metric(1)
	}

	if prevFrame < newFrame {
		// the event block will become a root, and move to the next frame
		// check how far from quorum the previous event block is to know far much progress new event block made to complete the prev frame
		prevRoots := h.lachesis.Store.GetFrameRoots(prevFrame)
		for _, root := range prevRoots {
			prevFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, nil) //compute for prev event
			rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
			rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
			progress := prevFCProgress[0].Sum()
			if progress > prevFCProgress[0].Quorum {
				progress = prevFCProgress[0].Quorum // if more than a quorum, truncate to a quorum
			}
			prevFramePrevEventRootKnowledge += rootStake * progress

			newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event

			progress = newFCProgress[0].Sum()
			if progress > newFCProgress[0].Quorum {
				progress = newFCProgress[0].Quorum // if more than a quorum, truncate to a quorum
			}
			prevFrameNewEventRootKnowledge += rootStake * progress
		}
		// normalize to numbers in range [0,1]
		floatNew := float64(prevFrameNewEventRootKnowledge) / (float64(h.validators.Quorum()) * float64(h.validators.TotalWeight()))
		floatPrev := float64(prevFramePrevEventRootKnowledge) / (float64(h.validators.Quorum()) * float64(h.validators.TotalWeight()))
		// metric += math.Log10(floatNew) - math.Log10(floatPrev) // log10 gives orders of magnitude difference between new and prev
		metric += floatNew - floatPrev

		selfValidatorIdx := h.validators.GetIdx(h.dagi.GetEvent(h.SelfParentEvent).Creator())
		selfStake := h.validators.GetWeightByIdx(selfValidatorIdx)
		newFramePrevEventRootKnowledge = selfStake * selfStake
	}

	// Check progress in current frame
	newRoots := h.lachesis.Store.GetFrameRoots(newFrame)
	for _, root := range newRoots {
		rootValidatorIdx := h.validators.GetIdx(root.Slot.Validator)
		rootStake := h.validators.GetWeightByIdx(rootValidatorIdx)
		if prevFrame == newFrame {
			prevFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, nil) //compute for prev event
			progress := prevFCProgress[0].Sum()
			if progress > prevFCProgress[0].Quorum {
				progress = prevFCProgress[0].Quorum // if more than a quorum, truncate to a quorum
			}
			newFramePrevEventRootKnowledge += rootStake * progress
		}

		newFCProgress := h.lachesis.DagIndex.ForklessCauseProgress(h.SelfParentEvent, root.ID, nil, chosenHeads) //compute for new event

		progress := newFCProgress[0].Sum()
		if progress > newFCProgress[0].Quorum {
			progress = newFCProgress[0].Quorum // if more than a quorum, truncate to a quorum
		}
		newFrameNewEventRootKnowledge += rootStake * progress
	}
	// normalize to numbers in range [0,1]
	floatNew := float64(newFrameNewEventRootKnowledge) / (float64(h.validators.Quorum()) * float64(h.validators.TotalWeight()))
	floatPrev := float64(newFramePrevEventRootKnowledge) / (float64(h.validators.Quorum()) * float64(h.validators.TotalWeight()))

	// if root knowledge grows (approximately) exponentially with frame sequence number, then the log (or number of orders of magnitude)
	// difference between events should be roughly constant, independent of where the new and prev frames are in the frame sequence
	// (e.g. early or late in the frame)
	// metric += math.Log10(floatNew) - math.Log10(floatPrev) // log10 gives orders of magnitude difference between new and prev
	metric += floatNew - floatPrev

	// For the purposes of producing a metric in the range [0,1] the below centers the metric distribution to mean = 1, and standard deviation 0.5
	// if the metric data are normally distributed then about 84% of adjustedMetrics will be > 0.5
	adjustedMetric := (metric - h.metricStats.expMovAv + 1) * (0.5 / math.Sqrt(h.metricStats.expMovVar))
	if adjustedMetric > 1 {
		// any above average result has value 1
		adjustedMetric = 1
	}
	if adjustedMetric < 0 {
		// minimum value is 0
		adjustedMetric = 0
	}

	// Collect statistics on the metric, so that the size of any metric can be compard to past distribution.
	// compute the deviation from the mean, divided by standard deviation, i.e. (metric - mean(metric))/std(metric)
	// The data may be non-stationary over time, so we can use an exponential moving average, and moving variance
	h.updateMetricStats(metric) // +++TODO should only adjust this when the event is actually emitted? Not just when calculating
	// fmt.Println("Raw Metric: ", metric, " Av: ", h.metricStats.expMovAv, " Var: ", h.metricStats.expMovVar)
	// fmt.Print(metric, ", ")
	// return Metric(adjustedMetric)
	return Metric(metric * 1e6)
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
		vecClock[i] = h.dagi.GetMergedHighestBefore(parent)
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
