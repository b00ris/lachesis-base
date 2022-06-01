package vecfc

import (
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

type kv struct {
	a, b hash.Event
}

// ForklessCause calculates "sufficient coherence" between the events.
// The A.HighestBefore array remembers the sequence number of the last
// event by each validator that is an ancestor of A. The array for
// B.LowestAfter remembers the sequence number of the earliest
// event by each validator that is a descendant of B. Compare the two arrays,
// and find how many elements in the A.HighestBefore array are greater
// than or equal to the corresponding element of the B.LowestAfter
// array. If there are more than 2n/3 such matches, then the A and B
// have achieved sufficient coherency.
//
// If B1 and B2 are forks, then they cannot BOTH forkless-cause any specific event A,
// unless more than 1/3W are Byzantine.
// This great property is the reason why this function exists,
// providing the base for the BFT algorithm.
func (vi *Index) ForklessCause(aID, bID hash.Event) bool {
	if res, ok := vi.cache.ForklessCause.Get(kv{aID, bID}); ok {
		return res.(bool)
	}

	vi.Engine.InitBranchesInfo()
	res := vi.forklessCause(aID, bID)

	vi.cache.ForklessCause.Add(kv{aID, bID}, res, 1)
	return res
}

func (vi *Index) forklessCause(aID, bID hash.Event) bool {
	// Get events by hash
	a := vi.GetHighestBefore(aID)
	if a == nil {
		vi.crit(fmt.Errorf("Event A=%s not found", aID.String()))
		return false
	}

	// check A doesn't observe any forks from B
	if vi.Engine.AtLeastOneFork() {
		bBranchID := vi.Engine.GetEventBranchID(bID)
		if a.Get(bBranchID).IsForkDetected() { // B is observed as cheater by A
			return false
		}
	}

	// check A observes that {QUORUM} non-cheater-validators observe B
	b := vi.GetLowestAfter(bID)
	if b == nil {
		vi.crit(fmt.Errorf("Event B=%s not found", bID.String()))
		return false
	}

	yes := vi.validators.NewCounter()
	// calculate forkless causing using the indexes
	branchIDs := vi.Engine.BranchesInfo().BranchIDCreatorIdxs
	for branchIDint, creatorIdx := range branchIDs {
		branchID := idx.Validator(branchIDint)

		// bLowestAfter := vi.GetLowestAfterSeq_(bID, branchID)   // lowest event from creator on branchID, which observes B
		bLowestAfter := b.Get(branchID)   // lowest event from creator on branchID, which observes B
		aHighestBefore := a.Get(branchID) // highest event from creator, observed by A

		// if lowest event from branchID which observes B <= highest from branchID observed by A
		// then {highest from branchID observed by A} observes B
		if bLowestAfter <= aHighestBefore.Seq && bLowestAfter != 0 && !aHighestBefore.IsForkDetected() {
			// we may count the same creator multiple times (on different branches)!
			// so not every call increases the counter
			yes.CountByIdx(creatorIdx)
		}
	}
	return yes.HasQuorum()
}

func (vi *Index) ForklessCauseProgress(aID, bID hash.Event, heads, chosenHeads hash.Events) []*pos.WeightCounter {
	// For each head in heads, find ForklessCause(a,b) if a head is selected as a parent for a, chosenheads are the already selected parents
	// +++todo, fix error handling returns
	headsFCProgress := make([]*pos.WeightCounter, len(heads)+1) // last entry will be for the result without any new head

	// Get events by hash
	a := vi.GetHighestBefore(aID)
	if a == nil {
		vi.crit(fmt.Errorf("Event A=%s not found", aID.String()))
		return headsFCProgress
	}

	c := make([]*HighestBeforeSeq, len(heads))
	for i, _ := range heads {
		c[i] = vi.GetHighestBefore(heads[i])
		if c[i] == nil {
			vi.crit(fmt.Errorf("Event Head=%s not found", heads[i].String()))
			return headsFCProgress
		}
	}

	d := make([]*HighestBeforeSeq, len(chosenHeads))
	for i, _ := range chosenHeads {
		d[i] = vi.GetHighestBefore(chosenHeads[i])
		if d[i] == nil {
			vi.crit(fmt.Errorf("Event Chosen Head=%s not found", chosenHeads[i].String()))
			return headsFCProgress
		}
	}

	// check A doesn't observe any forks from B
	if vi.Engine.AtLeastOneFork() {
		bBranchID := vi.Engine.GetEventBranchID(bID)
		if a.Get(bBranchID).IsForkDetected() { // B is observed as cheater by A
			return headsFCProgress
		}
	}

	// check D doesn't observe any forks from B
	for i := 0; i < len(d); i++ {
		if vi.Engine.AtLeastOneFork() {
			bBranchID := vi.Engine.GetEventBranchID(bID)
			if d[i].Get(bBranchID).IsForkDetected() { // B is observed as cheater by D
				return headsFCProgress
			}
		}
	}

	b := vi.GetLowestAfter(bID)
	if b == nil {
		vi.crit(fmt.Errorf("Event B=%s not found", bID.String()))
		return headsFCProgress
	}

	for i, _ := range headsFCProgress {
		headsFCProgress[i] = vi.validators.NewCounter()
	}

	// calculate forkless causing using the indexes
	branchIDs := vi.Engine.BranchesInfo().BranchIDCreatorIdxs
	for branchIDint, creatorIdx := range branchIDs {
		branchID := idx.Validator(branchIDint)

		// bLowestAfter := vi.GetLowestAfterSeq_(bID, branchID)   // lowest event from creator on branchID, which observes B
		bLowestAfter := b.Get(branchID)  // lowest event from creator on branchID, which observes B
		HighestBefore := a.Get(branchID) // highest event from creator, observed by A

		IsForkDetected := HighestBefore.IsForkDetected()

		for i, _ := range chosenHeads {
			dHighestBefore := d[i].Get(branchID) // highest event from creator, observed by A
			HighestBefore.Seq = maxEvent(HighestBefore.Seq, dHighestBefore.Seq)
			IsForkDetected = IsForkDetected || dHighestBefore.IsForkDetected()
		}

		// first do forkless cause for no head (only selected heads and a)
		if bLowestAfter <= HighestBefore.Seq && bLowestAfter != 0 && !IsForkDetected {
			// we may count the same creator multiple times (on different branches)!
			// so not every call increases the counter
			headsFCProgress[len(headsFCProgress)-1].CountByIdx(creatorIdx)
		}
		// now do forkless cause for each head with selected heads and a
		for i, _ := range heads {
			cHighestBefore := c[i].Get(branchID)
			cIsForkDetected := IsForkDetected || cHighestBefore.IsForkDetected()
			cHighestBefore.Seq = maxEvent(HighestBefore.Seq, cHighestBefore.Seq)

			if bLowestAfter <= cHighestBefore.Seq && bLowestAfter != 0 && !cIsForkDetected {
				// we may count the same creator multiple times (on different branches)!
				// so not every call increases the counter
				headsFCProgress[i].CountByIdx(creatorIdx)
			}
		}
	}
	return headsFCProgress
}

func maxEvent(a idx.Event, b idx.Event) idx.Event {
	if a > b {
		return a
	}
	return b
}
