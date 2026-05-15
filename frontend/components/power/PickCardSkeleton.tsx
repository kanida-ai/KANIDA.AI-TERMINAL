/**
 * PickCardSkeleton — placeholder while a Pick is loading.
 * Matches the expanded PickCard's vertical rhythm so layout doesn't jump.
 */
export function PickCardSkeleton() {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded-lg p-4 md:p-5 space-y-4 animate-pulse">
      {/* header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="h-4 w-6 bg-neutral-800 rounded" />
          <div className="h-6 w-24 bg-neutral-800 rounded" />
          <div className="h-4 w-32 bg-neutral-800 rounded hidden md:block" />
        </div>
        <div className="flex items-center gap-2">
          <div className="h-6 w-20 bg-neutral-800 rounded" />
          <div className="h-4 w-16 bg-neutral-800 rounded hidden md:block" />
        </div>
      </div>
      {/* story */}
      <div className="space-y-1.5">
        <div className="h-3 bg-neutral-800 rounded w-full" />
        <div className="h-3 bg-neutral-800 rounded w-11/12" />
        <div className="h-3 bg-neutral-800 rounded w-3/4" />
      </div>
      {/* patterns */}
      <div className="space-y-2">
        <div className="h-2.5 w-32 bg-neutral-800 rounded" />
        {[0, 1, 2].map(i => (
          <div key={i} className="space-y-1.5">
            <div className="h-3 bg-neutral-800 rounded w-5/6" />
            <div className="h-3 bg-neutral-800 rounded w-2/3 ml-5" />
          </div>
        ))}
      </div>
      {/* outcomes grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {[0, 1].map(i => (
          <div key={i} className="bg-neutral-950/60 border border-neutral-800 rounded p-3 space-y-2">
            <div className="h-2.5 w-28 bg-neutral-800 rounded" />
            {[0, 1, 2].map(j => (
              <div key={j} className="h-3 bg-neutral-800 rounded w-full" />
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}
