package job

type Option func(*Runner)

func WithLoggerProvider(provider LoggerProvider) Option {
	return func(r *Runner) {
		if provider == nil {
			provider = newStdLoggerProvider()
		}
		r.loggerProvider = provider
		r.propagateLoggerProvider()
	}
}

func WithErrorHandler(handler func(Task, error)) Option {
	return func(fsjr *Runner) {
		if handler != nil {
			fsjr.errorHandler = handler
		}
	}
}

func WithRegistry(registry Registry) Option {
	return func(fsjr *Runner) {
		if registry != nil {
			fsjr.registry = registry
		}
	}
}

func WithMetadataParser(parser MetadataParser) Option {
	return func(fsjr *Runner) {
		if parser != nil {
			fsjr.parser = parser
		}
	}
}

func WithTaskCreator(creator TaskCreator) Option {
	return func(r *Runner) {
		if creator != nil {
			r.attachTaskCreatorOptions(creator)
			r.taskCreators = append(r.taskCreators, creator)
		}
	}
}

func WithTaskIDProvider(provider TaskIDProvider) Option {
	return func(r *Runner) {
		r.taskIDProvider = provider
		r.propagateTaskIDProvider()
	}
}

func WithTaskEventHandler(handler TaskEventHandler) Option {
	return func(r *Runner) {
		if handler == nil {
			return
		}
		r.taskEventHandlers = append(r.taskEventHandlers, handler)
		r.propagateTaskEventHandler(handler)
	}
}
