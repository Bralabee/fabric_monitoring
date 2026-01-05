import { useState, useEffect } from 'react'
import { Outlet, Link, useLocation } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { 
  Search, 
  Menu, 
  X, 
  Moon, 
  Sun,
  Rocket,
  BarChart3,
  Shield,
  Boxes,
  Cloud,
  Wrench,
  Home
} from 'lucide-react'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { fetchCategories, type Category } from '@/lib/api'
import { cn } from '@/lib/utils'

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  rocket: Rocket,
  'chart-bar': BarChart3,
  'shield-check': Shield,
  cube: Boxes,
  'cloud-arrow-up': Cloud,
  wrench: Wrench,
}

export function Layout() {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return document.documentElement.classList.contains('dark')
    }
    return false
  })
  const [searchQuery, setSearchQuery] = useState('')
  const location = useLocation()

  const { data: categories } = useQuery({
    queryKey: ['categories'],
    queryFn: fetchCategories,
  })

  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }, [darkMode])

  // Close sidebar on route change
  useEffect(() => {
    setSidebarOpen(false)
  }, [location])

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (searchQuery.trim()) {
      window.location.href = `/search?q=${encodeURIComponent(searchQuery)}`
    }
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="container flex h-14 items-center">
          <Button
            variant="ghost"
            size="icon"
            className="mr-2 md:hidden"
            onClick={() => setSidebarOpen(!sidebarOpen)}
          >
            {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
          </Button>
          
          <Link to="/" className="flex items-center space-x-2">
            <BarChart3 className="h-6 w-6 text-primary" />
            <span className="font-bold hidden sm:inline-block">
              Fabric Monitoring Guide
            </span>
          </Link>

          <div className="flex flex-1 items-center justify-end space-x-4">
            <form onSubmit={handleSearch} className="w-full max-w-sm">
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  type="search"
                  placeholder="Search guides..."
                  className="pl-8"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
              </div>
            </form>
            
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setDarkMode(!darkMode)}
            >
              {darkMode ? (
                <Sun className="h-5 w-5" />
              ) : (
                <Moon className="h-5 w-5" />
              )}
            </Button>
          </div>
        </div>
      </header>

      <div className="container flex-1 items-start md:grid md:grid-cols-[220px_minmax(0,1fr)] md:gap-6 lg:grid-cols-[240px_minmax(0,1fr)] lg:gap-10">
        {/* Sidebar */}
        <aside
          className={cn(
            "fixed top-14 z-30 -ml-2 hidden h-[calc(100vh-3.5rem)] w-full shrink-0 md:sticky md:block",
            sidebarOpen && "block"
          )}
        >
          <div className="h-full py-6 pr-6 lg:py-8 overflow-y-auto">
            <nav className="grid items-start gap-2">
              <Link
                to="/"
                className={cn(
                  "flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-all hover:bg-accent",
                  location.pathname === "/" && "bg-accent"
                )}
              >
                <Home className="h-4 w-4" />
                Home
              </Link>
              
              {categories?.map((category: Category) => {
                const Icon = iconMap[category.icon] || Boxes
                return (
                  <div key={category.id} className="mt-4">
                    <h4 className="mb-1 rounded-md px-2 py-1 text-sm font-semibold flex items-center gap-2">
                      <Icon className="h-4 w-4" />
                      {category.title}
                    </h4>
                    <div className="grid gap-1">
                      {category.scenarios.map((scenario) => (
                        <Link
                          key={scenario.id}
                          to={`/scenario/${scenario.id}`}
                          className={cn(
                            "group flex items-center rounded-lg px-3 py-2 text-sm text-muted-foreground transition-all hover:text-foreground hover:bg-accent",
                            location.pathname === `/scenario/${scenario.id}` &&
                              "bg-accent text-foreground"
                          )}
                        >
                          {scenario.title}
                        </Link>
                      ))}
                    </div>
                  </div>
                )
              })}
            </nav>
          </div>
        </aside>

        {/* Mobile sidebar overlay */}
        {sidebarOpen && (
          <div
            className="fixed inset-0 top-14 z-20 bg-background/80 backdrop-blur-sm md:hidden"
            onClick={() => setSidebarOpen(false)}
          >
            <aside className="fixed top-14 left-0 z-30 h-[calc(100vh-3.5rem)] w-64 bg-background border-r p-4 overflow-y-auto">
              <nav className="grid items-start gap-2">
                <Link
                  to="/"
                  className={cn(
                    "flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-all hover:bg-accent",
                    location.pathname === "/" && "bg-accent"
                  )}
                >
                  <Home className="h-4 w-4" />
                  Home
                </Link>
                
                {categories?.map((category: Category) => {
                  const Icon = iconMap[category.icon] || Boxes
                  return (
                    <div key={category.id} className="mt-4">
                      <h4 className="mb-1 rounded-md px-2 py-1 text-sm font-semibold flex items-center gap-2">
                        <Icon className="h-4 w-4" />
                        {category.title}
                      </h4>
                      <div className="grid gap-1">
                        {category.scenarios.map((scenario) => (
                          <Link
                            key={scenario.id}
                            to={`/scenario/${scenario.id}`}
                            className={cn(
                              "group flex items-center rounded-lg px-3 py-2 text-sm text-muted-foreground transition-all hover:text-foreground hover:bg-accent",
                              location.pathname === `/scenario/${scenario.id}` &&
                                "bg-accent text-foreground"
                            )}
                          >
                            {scenario.title}
                          </Link>
                        ))}
                      </div>
                    </div>
                  )
                })}
              </nav>
            </aside>
          </div>
        )}

        {/* Main content */}
        <main className="relative py-6 lg:gap-10 lg:py-8">
          <Outlet />
        </main>
      </div>

      {/* Footer */}
      <footer className="border-t py-6 md:py-0">
        <div className="container flex flex-col items-center justify-between gap-4 md:h-16 md:flex-row">
          <p className="text-sm text-muted-foreground">
            USF Fabric Monitoring v0.3.16 · Interactive Guide
          </p>
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            <a
              href="https://github.com"
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-foreground"
            >
              GitHub
            </a>
            <a
              href="/docs"
              className="hover:text-foreground"
            >
              Documentation
            </a>
          </div>
        </div>
      </footer>
    </div>
  )
}
