package broker

type subscription[T interface{}] struct {
	ch       chan T
	blocking bool
}

func (s subscription[T]) send(msg T) {
	if s.blocking {
		s.ch <- msg
		return
	}

	select {
	case s.ch <- msg:
	default:
	}

	return
}

func (s subscription[T]) close() {
	close(s.ch)
}
