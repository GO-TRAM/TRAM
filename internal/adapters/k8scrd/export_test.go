package k8scrd

import "sigs.k8s.io/controller-runtime/pkg/cache"

func CacheOptions(opts Options) (cache.Options, error) {
	return cacheOptions(opts)
}
