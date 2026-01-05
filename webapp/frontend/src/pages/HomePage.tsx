import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { 
  Rocket, 
  BarChart3, 
  Shield, 
  Boxes, 
  Cloud, 
  Wrench,
  ArrowRight,
  Clock,
  BookOpen
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { fetchCategories, type Category } from '@/lib/api'

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  rocket: Rocket,
  'chart-bar': BarChart3,
  'shield-check': Shield,
  cube: Boxes,
  'cloud-arrow-up': Cloud,
  wrench: Wrench,
}

const difficultyColors: Record<string, string> = {
  beginner: 'success',
  intermediate: 'default',
  advanced: 'destructive',
}

export function HomePage() {
  const { data: categories, isLoading, error } = useQuery({
    queryKey: ['categories'],
    queryFn: fetchCategories,
  })

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto"></div>
          <p className="mt-4 text-muted-foreground">Loading guides...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="text-center text-destructive">
          <p>Failed to load guides. Please try again later.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      {/* Hero Section */}
      <div className="space-y-4">
        <h1 className="text-4xl font-bold tracking-tight">
          USF Fabric Monitoring
          <span className="text-primary"> Interactive Guide</span>
        </h1>
        <p className="text-xl text-muted-foreground max-w-3xl">
          A comprehensive, step-by-step guide to mastering the USF Fabric Monitoring toolkit.
          From initial setup to advanced analytics, these guides will walk you through
          every aspect of the system.
        </p>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-4">
              <div className="p-2 bg-primary/10 rounded-lg">
                <BookOpen className="h-6 w-6 text-primary" />
              </div>
              <div>
                <p className="text-2xl font-bold">
                  {categories?.reduce((acc, cat) => acc + cat.scenarios.length, 0) || 0}
                </p>
                <p className="text-sm text-muted-foreground">Scenarios</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-4">
              <div className="p-2 bg-primary/10 rounded-lg">
                <Boxes className="h-6 w-6 text-primary" />
              </div>
              <div>
                <p className="text-2xl font-bold">{categories?.length || 0}</p>
                <p className="text-sm text-muted-foreground">Categories</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-4">
              <div className="p-2 bg-primary/10 rounded-lg">
                <Clock className="h-6 w-6 text-primary" />
              </div>
              <div>
                <p className="text-2xl font-bold">~3-4</p>
                <p className="text-sm text-muted-foreground">Hours Total</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Categories */}
      <div className="space-y-8">
        {categories?.map((category: Category) => {
          const Icon = iconMap[category.icon] || Boxes
          return (
            <div key={category.id} className="space-y-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-primary/10 rounded-lg">
                  <Icon className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <h2 className="text-2xl font-semibold">{category.title}</h2>
                  <p className="text-muted-foreground">{category.description}</p>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {category.scenarios.map((scenario) => (
                  <Card key={scenario.id} className="group hover:border-primary/50 transition-colors">
                    <CardHeader>
                      <div className="flex items-start justify-between">
                        <CardTitle className="text-lg">{scenario.title}</CardTitle>
                        <Badge variant={difficultyColors[scenario.difficulty] as any}>
                          {scenario.difficulty}
                        </Badge>
                      </div>
                      <CardDescription>{scenario.description}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4 text-sm text-muted-foreground">
                          <span className="flex items-center gap-1">
                            <BookOpen className="h-4 w-4" />
                            {scenario.step_count} steps
                          </span>
                          <span className="flex items-center gap-1">
                            <Clock className="h-4 w-4" />
                            {scenario.estimated_time}
                          </span>
                        </div>
                        <Link to={`/scenario/${scenario.id}`}>
                          <Button variant="ghost" size="sm" className="group-hover:bg-primary group-hover:text-primary-foreground">
                            Start
                            <ArrowRight className="ml-2 h-4 w-4" />
                          </Button>
                        </Link>
                      </div>
                      {scenario.tags.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-4">
                          {scenario.tags.slice(0, 4).map((tag) => (
                            <Badge key={tag} variant="outline" className="text-xs">
                              {tag}
                            </Badge>
                          ))}
                          {scenario.tags.length > 4 && (
                            <Badge variant="outline" className="text-xs">
                              +{scenario.tags.length - 4}
                            </Badge>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                ))}
              </div>
            </div>
          )
        })}
      </div>

      {/* Getting Started CTA */}
      <Card className="bg-primary/5 border-primary/20">
        <CardContent className="pt-6">
          <div className="flex flex-col md:flex-row items-center gap-6">
            <div className="p-4 bg-primary/10 rounded-full">
              <Rocket className="h-10 w-10 text-primary" />
            </div>
            <div className="flex-1 text-center md:text-left">
              <h3 className="text-xl font-semibold">New to Fabric Monitoring?</h3>
              <p className="text-muted-foreground mt-1">
                Start with the Getting Started guide to set up your environment and learn the basics.
              </p>
            </div>
            <Link to="/scenario/getting-started">
              <Button size="lg">
                Get Started
                <ArrowRight className="ml-2 h-5 w-5" />
              </Button>
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
