/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"context"
	"sync"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

type DoWorkPieceFunc func(piece int)

// ParallelizeUntil is a framework that allows for parallelizing N
// independent pieces of work until done or the context is canceled.
func ParallelizeUntil(ctx context.Context, workers, pieces int, doWorkPiece DoWorkPieceFunc) {
	var stop <-chan struct{}
	if ctx != nil {
		stop = ctx.Done()
	}

	if pieces < workers {
		workers = pieces
	}
	distro := make([][]int, workers)
	// establish the slices
	for i := 0; i < workers; i++ {
		distro = append(distro, []int{i})
	}
	// distribute the rest of the work
	for i := workers; i < pieces; i++ {
		idx := i % workers
		distro[idx] = append(distro[idx], i)
	}
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(pieces []int) {
			defer utilruntime.HandleCrash()
			defer wg.Done()
			for _, piece := range pieces {
				select {
				case <-stop:
					return
				default:
					doWorkPiece(piece)
				}
			}
		}(distro[i])
	}
	wg.Wait()
}
