package job

type Option func(*Runner)

func WithErrorHandler(handler func(error)) Option {
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
			r.taskCreators = append(r.taskCreators, creator)
		}
	}
}
