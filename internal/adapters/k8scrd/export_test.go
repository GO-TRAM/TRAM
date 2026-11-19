// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd

import "sigs.k8s.io/controller-runtime/pkg/cache"

func CacheOptions(opts Options) (cache.Options, error) {
	return cacheOptions(opts)
}

func WithoutNameValidation(opts Options) Options {
	opts.skipNameValidation = true
	return opts
}

func ManagerWithoutNameValidation(opts ManagerOptions) ManagerOptions {
	opts.skipNameValidation = true
	return opts
}
