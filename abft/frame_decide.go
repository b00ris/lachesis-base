package abft

import (
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/utils/perfl"
)

// onFrameDecided moves LastDecidedFrameN to frame.
// It includes: moving current decided frame, txs ordering and execution, epoch sealing.
func (p *Orderer) onFrameDecided(frame idx.Frame, atropos hash.Event) (bool, error) {
	// new checkpoint
	start := time.Now()
	var newValidators *pos.Validators
	if p.callback.ApplyAtropos != nil {
		start := time.Now()
		newValidators = p.callback.ApplyAtropos(frame, atropos)
		perfl.Log("ApplyAtropos", time.Since(start))
	}

	lastDecidedState := *p.store.GetLastDecidedState()
	if newValidators != nil {
		lastDecidedState.LastDecidedFrame = FirstFrame - 1
		err := p.sealEpoch(newValidators)
		if err != nil {
			return true, err
		}
		p.election.Reset(newValidators, FirstFrame)
	} else {
		lastDecidedState.LastDecidedFrame = frame
		p.election.Reset(p.store.GetValidators(), frame+1)
	}
	p.store.SetLastDecidedState(&lastDecidedState)
	perfl.Log("onFrameDecided", time.Since(start))
	return newValidators != nil, nil
}

func (p *Orderer) resetEpochStore(newEpoch idx.Epoch) error {
	err := p.store.dropEpochDB()
	if err != nil {
		return err
	}
	err = p.store.openEpochDB(newEpoch)
	if err != nil {
		return err
	}

	if p.callback.EpochDBLoaded != nil {
		p.callback.EpochDBLoaded(newEpoch)
	}
	return nil
}

func (p *Orderer) sealEpoch(newValidators *pos.Validators) error {
	// new PrevEpoch state
	epochState := *p.store.GetEpochState()
	epochState.Epoch++
	epochState.Validators = newValidators
	p.store.SetEpochState(&epochState)

	return p.resetEpochStore(epochState.Epoch)
}
