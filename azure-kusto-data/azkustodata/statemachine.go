package azkustodata

// statemachine.go provides statemachines for interpreting frame streams for varying Kusto options.
// Based on the standard Go statemachine design by Rob Pike.

import (
	"context"
	"fmt"
	"sync"

	"azure-kusto-go/azure-kusto-data/azkustodata/errors"
)

// stateFn represents a function that executes at a given state.
type stateFn func() (stateFn, error)

// stateMachine provides a state machine for executing a of of well defined states.
type stateMachine interface {
	// start starts the stateMachine and returns either the next state to run, an error, or nil, nil.
	start() (stateFn, error)
	rowIter() *RowIterator
}

// runSM runs a stateMachine to its conclusion.
func runSM(sm stateMachine) {
	defer close(sm.rowIter().inRows)

	var fn = sm.start
	var err error
	for {
		fn, err = fn()
		switch {
		case err != nil:
			sm.rowIter().inErr <- err
			return
		case fn == nil && err == nil:
			return
		}
	}
}

// nonProgressiveSM implements a stateMachine that processes Kusto data that is not non-streamibng.
type nonProgressiveSM struct {
	op            errors.Op
	iter          *RowIterator
	in            chan frame
	columnSetOnce sync.Once
	ctx           context.Context
	hasCompletion bool
}

func (d *nonProgressiveSM) start() (stateFn, error) {
	return d.process, nil
}

func (d *nonProgressiveSM) rowIter() *RowIterator {
	return d.iter
}

func (d *nonProgressiveSM) process() (sf stateFn, err error) {
	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	case fr, ok := <-d.in:
		if !ok {
			if !d.hasCompletion {
				return nil, errors.E(d.op, errors.KInternal, fmt.Errorf("non-progressive stream did not have dataSetCompletion frame"))
			}
			return nil, nil
		}

		if d.hasCompletion {
			return nil, errors.E(d.op, errors.KInternal, fmt.Errorf("saw a dataSetCompletion frame, then received a %T frame", fr))
		}

		switch table := fr.(type) {
		case dataTable:
			switch table.TableKind {
			case tkPrimaryResult:
				// syncs the flow, waiting for columns to be decoded
				d.columnSetOnce.Do(func() {
					d.iter.inColumns <- table.Columns
				})

				select {
				case <-d.ctx.Done():
					return nil, d.ctx.Err()
				case d.iter.inRows <- table.Rows:
				}
			default:
				select {
				case <-d.ctx.Done():
					return nil, d.ctx.Err()
				case d.iter.inNonPrimary <- table:
				}
			}
		case errorFrame:
			return nil, table
		case dataSetCompletion:
			select {
			case <-d.ctx.Done():
				return nil, d.ctx.Err()
			case d.iter.inCompletion <- table:
			}
			d.hasCompletion = true
		}
	}
	return d.process, nil
}

// progressiveSM implements a stateMachine that handles progressive streaming Kusto data.
type progressiveSM struct {
	op            errors.Op
	iter          *RowIterator
	in            chan frame
	columnSetOnce sync.Once
	ctx           context.Context

	currentHeader *tableHeader
	currentFrame  frame
	nonPrimary    *dataTable
}

func (p *progressiveSM) start() (stateFn, error) {
	return p.nextFrame, nil
}

func (p *progressiveSM) rowIter() *RowIterator {
	return p.iter
}

func (p *progressiveSM) nextFrame() (stateFn, error) {
	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case fr, ok := <-p.in:
		if !ok {
			return nil, errors.E(p.op, errors.KInternal, fmt.Errorf("received a table stream that did not finish before our input channel"))
		}
		p.currentFrame = fr
		switch table := fr.(type) {
		case dataTable:
			return p.dataTable, nil
		case dataSetCompletion:
			return p.dataSetCompletion, nil
		case tableHeader:
			return p.tableHeader, nil
		case tableFragment:
			return p.fragment, nil
		case tableProgress:
			return p.progress, nil
		case tableCompletion:
			return p.completion, nil
		case errorFrame:
			return nil, table
		default:
			return nil, errors.E(p.op, errors.KInternal, fmt.Errorf("received an unknown frame in a progressive table stream we didn't understand: %T", table))
		}
	}
}

func (p *progressiveSM) dataTable() (stateFn, error) {
	if p.currentHeader != nil {
		return nil, errors.ES(p.op, errors.KInternal, "received a DatTable between a tableHeader and TableCompletion")
	}
	table := p.currentFrame.(dataTable)
	if table.TableKind == tkPrimaryResult {
		return nil, errors.ES(p.op, errors.KInternal, "progressive stream had dataTable with Kind == PrimaryResult")
	}

	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case p.iter.inNonPrimary <- table:
	}

	return p.nextFrame, nil
}

func (p *progressiveSM) dataSetCompletion() (stateFn, error) {
	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case p.iter.inCompletion <- p.currentFrame.(dataSetCompletion):
	}

	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case frame, ok := <-p.in:
		if !ok {
			return nil, nil
		}
		return nil, errors.ES(p.op, errors.KInternal, "recieved a dataSetCompletion frame and then a %T frame", frame)
	}
}

func (p *progressiveSM) tableHeader() (stateFn, error) {
	table := p.currentFrame.(tableHeader)
	p.currentHeader = &table
	if p.currentHeader.TableKind == tkPrimaryResult {
		p.columnSetOnce.Do(func() {
			p.iter.inColumns <- table.Columns
		})
	} else {
		p.nonPrimary = &dataTable{
			baseFrame: baseFrame{FrameType: ftDataTable},
			TableID:   p.currentHeader.TableID,
			TableKind: p.currentHeader.TableKind,
			TableName: p.currentHeader.TableName,
			Columns:   p.currentHeader.Columns,
		}
	}

	return p.nextFrame, nil
}

func (p *progressiveSM) fragment() (stateFn, error) {
	if p.currentHeader == nil {
		return nil, errors.ES(p.op, errors.KInternal, "received a TableFragment without a tableHeader")
	}

	if p.currentHeader.TableKind == tkPrimaryResult {
		table := p.currentFrame.(tableFragment)

		select {
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		case p.iter.inRows <- table.Rows:
		}
	} else {
		p.nonPrimary.Rows = append(p.nonPrimary.Rows, p.currentFrame.(tableFragment).Rows...)
	}
	return p.nextFrame, nil
}

func (p *progressiveSM) progress() (stateFn, error) {
	if p.currentHeader == nil {
		return nil, errors.ES(p.op, errors.KInternal, "received a TableProgress without a tableHeader")
	}
	p.iter.inProgress <- p.currentFrame.(tableProgress)
	return p.nextFrame, nil
}

func (p *progressiveSM) completion() (stateFn, error) {
	if p.currentHeader == nil {
		return nil, errors.ES(p.op, errors.KInternal, "received a TableCompletion without a tableHeader")
	}
	if p.currentHeader.TableKind == tkPrimaryResult {
		// Do nothing here.
		//p.iter.inCompletion <-p.currentFrame.(TableCompletion)
	} else {
		p.iter.inNonPrimary <- *p.nonPrimary
	}
	p.nonPrimary = nil
	p.currentHeader = nil
	p.currentFrame = nil

	return p.nextFrame, nil
}
