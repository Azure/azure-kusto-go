package utils

type RWMutex interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

type FakeMutex struct {
}

func (m *FakeMutex) RLock() {
}

func (m *FakeMutex) RUnlock() {
}

func (m *FakeMutex) Lock() {
}

func (m *FakeMutex) Unlock() {
}
