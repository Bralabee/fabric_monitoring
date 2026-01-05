import { useState, useEffect } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Search, ArrowRight, FileText } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { searchScenarios, type SearchResult } from '@/lib/api'

const difficultyColors: Record<string, string> = {
  beginner: 'success',
  intermediate: 'default',
  advanced: 'destructive',
}

export function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const initialQuery = searchParams.get('q') || ''
  const [query, setQuery] = useState(initialQuery)
  const [debouncedQuery, setDebouncedQuery] = useState(initialQuery)

  // Debounce search query
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(query)
      if (query) {
        setSearchParams({ q: query })
      } else {
        setSearchParams({})
      }
    }, 300)
    return () => clearTimeout(timer)
  }, [query, setSearchParams])

  const { data: results, isLoading } = useQuery({
    queryKey: ['search', debouncedQuery],
    queryFn: () => searchScenarios(debouncedQuery),
    enabled: debouncedQuery.length >= 2,
  })

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    setDebouncedQuery(query)
    if (query) {
      setSearchParams({ q: query })
    }
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="space-y-4">
        <h1 className="text-3xl font-bold tracking-tight">Search Guides</h1>
        <p className="text-muted-foreground">
          Find guides, steps, and information across all scenarios.
        </p>
      </div>

      {/* Search Form */}
      <form onSubmit={handleSearch} className="max-w-2xl">
        <div className="relative">
          <Search className="absolute left-3 top-3 h-5 w-5 text-muted-foreground" />
          <Input
            type="search"
            placeholder="Search for topics, commands, concepts..."
            className="pl-10 h-12 text-lg"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            autoFocus
          />
        </div>
      </form>

      {/* Results */}
      <div className="space-y-4">
        {isLoading && debouncedQuery && (
          <div className="flex items-center gap-3 text-muted-foreground">
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-primary"></div>
            Searching...
          </div>
        )}

        {!isLoading && debouncedQuery && results && (
          <div className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Found {results.length} result{results.length !== 1 ? 's' : ''} for "{debouncedQuery}"
            </p>

            {results.length === 0 && (
              <Card>
                <CardContent className="pt-6">
                  <div className="text-center py-8">
                    <FileText className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <h3 className="text-lg font-medium">No results found</h3>
                    <p className="text-muted-foreground mt-2">
                      Try adjusting your search terms or browse the guides from the home page.
                    </p>
                    <Link to="/">
                      <Button variant="outline" className="mt-4">
                        Browse All Guides
                      </Button>
                    </Link>
                  </div>
                </CardContent>
              </Card>
            )}

            {results.map((result: SearchResult) => (
              <Card key={`${result.scenario_id}-${result.step_id || 'main'}`} className="hover:border-primary/50 transition-colors">
                <CardHeader className="pb-2">
                  <div className="flex items-start justify-between gap-4">
                    <div className="space-y-1">
                      <div className="flex items-center gap-2 text-sm text-muted-foreground">
                        <Link 
                          to={`/scenario/${result.scenario_id}`}
                          className="hover:text-primary"
                        >
                          {result.scenario_title}
                        </Link>
                        {result.step_title && (
                          <>
                            <span>→</span>
                            <span>{result.step_title}</span>
                          </>
                        )}
                      </div>
                      <CardTitle className="text-lg">
                        {result.step_title || result.scenario_title}
                      </CardTitle>
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0">
                      <Badge variant="outline" className="text-xs">
                        {Math.round(result.relevance * 100)}% match
                      </Badge>
                      <Badge variant={difficultyColors[result.difficulty] as any}>
                        {result.difficulty}
                      </Badge>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  <p className="text-muted-foreground text-sm line-clamp-2">
                    {result.snippet}
                  </p>
                  <div className="flex items-center justify-between mt-4">
                    <div className="flex flex-wrap gap-2">
                      {result.matching_tags?.map((tag) => (
                        <Badge key={tag} variant="secondary" className="text-xs">
                          {tag}
                        </Badge>
                      ))}
                    </div>
                    <Link 
                      to={result.step_id 
                        ? `/scenario/${result.scenario_id}?step=${result.step_id}` 
                        : `/scenario/${result.scenario_id}`
                      }
                    >
                      <Button variant="ghost" size="sm">
                        View
                        <ArrowRight className="ml-2 h-4 w-4" />
                      </Button>
                    </Link>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        {!debouncedQuery && (
          <Card>
            <CardContent className="pt-6">
              <div className="text-center py-8">
                <Search className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-medium">Start searching</h3>
                <p className="text-muted-foreground mt-2">
                  Enter at least 2 characters to search across all guides.
                </p>
                <div className="flex flex-wrap justify-center gap-2 mt-4">
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => setQuery('conda')}
                  >
                    conda
                  </Button>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => setQuery('star schema')}
                  >
                    star schema
                  </Button>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => setQuery('authentication')}
                  >
                    authentication
                  </Button>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => setQuery('parquet')}
                  >
                    parquet
                  </Button>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => setQuery('workspace')}
                  >
                    workspace
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}
