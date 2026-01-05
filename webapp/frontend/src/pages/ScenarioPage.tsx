import { useState, useEffect } from 'react'
import { useParams, Link, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { 
  ChevronLeft, 
  ChevronRight, 
  Check, 
  Circle,
  Clock,
  BookOpen,
  AlertTriangle,
  Info,
  Lightbulb,
  XCircle
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { CodeBlock } from '@/components/CodeBlock'
import { 
  fetchScenario, 
  fetchProgress, 
  updateProgress,
} from '@/lib/api'
import { cn } from '@/lib/utils'

const difficultyColors: Record<string, string> = {
  beginner: 'success',
  intermediate: 'default',
  advanced: 'destructive',
}

const stepTypeIcons: Record<string, React.ComponentType<{ className?: string }>> = {
  concept: Info,
  action: Circle,
  warning: AlertTriangle,
  tip: Lightbulb,
  verification: Check,
}

export function ScenarioPage() {
  const { scenarioId } = useParams<{ scenarioId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [currentStepIndex, setCurrentStepIndex] = useState(0)

  const { data: scenario, isLoading: scenarioLoading, error: scenarioError } = useQuery({
    queryKey: ['scenario', scenarioId],
    queryFn: () => fetchScenario(scenarioId!),
    enabled: !!scenarioId,
  })

  const { data: progress } = useQuery({
    queryKey: ['progress', scenarioId],
    queryFn: () => fetchProgress(scenarioId!),
    enabled: !!scenarioId,
  })

  const progressMutation = useMutation({
    mutationFn: (stepId: string) => updateProgress(scenarioId!, stepId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['progress', scenarioId] })
    },
  })

  // Navigate to last incomplete step on load
  useEffect(() => {
    if (scenario && progress?.completed_steps) {
      const lastIncomplete = scenario.steps.findIndex(
        (step) => !progress.completed_steps.includes(step.id)
      )
      if (lastIncomplete !== -1 && lastIncomplete !== currentStepIndex) {
        setCurrentStepIndex(lastIncomplete)
      }
    }
  }, [scenario, progress])

  if (scenarioLoading) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto"></div>
          <p className="mt-4 text-muted-foreground">Loading guide...</p>
        </div>
      </div>
    )
  }

  if (scenarioError || !scenario) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="text-center">
          <XCircle className="h-12 w-12 text-destructive mx-auto" />
          <p className="mt-4 text-destructive">Failed to load guide</p>
          <Button variant="outline" className="mt-4" onClick={() => navigate('/')}>
            Back to Home
          </Button>
        </div>
      </div>
    )
  }

  const currentStep = scenario.steps[currentStepIndex]
  const completedSteps = progress?.completed_steps || []
  const progressPercentage = (completedSteps.length / scenario.steps.length) * 100
  const isCurrentStepComplete = completedSteps.includes(currentStep.id)

  const handleMarkComplete = () => {
    progressMutation.mutate(currentStep.id)
    if (currentStepIndex < scenario.steps.length - 1) {
      setCurrentStepIndex(currentStepIndex + 1)
    }
  }

  const handlePrevious = () => {
    if (currentStepIndex > 0) {
      setCurrentStepIndex(currentStepIndex - 1)
    }
  }

  const handleNext = () => {
    if (currentStepIndex < scenario.steps.length - 1) {
      setCurrentStepIndex(currentStepIndex + 1)
    }
  }

  const StepIcon = stepTypeIcons[currentStep.type] || Circle

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-muted-foreground">
        <Link to="/" className="hover:text-foreground">Home</Link>
        <ChevronRight className="h-4 w-4" />
        <span className="text-foreground">{scenario.title}</span>
      </nav>

      {/* Header */}
      <div className="space-y-4">
        <div className="flex items-start justify-between flex-wrap gap-4">
          <div className="space-y-1">
            <h1 className="text-3xl font-bold tracking-tight">{scenario.title}</h1>
            <p className="text-lg text-muted-foreground">{scenario.description}</p>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant={difficultyColors[scenario.difficulty] as any}>
              {scenario.difficulty}
            </Badge>
            <Badge variant="outline" className="flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {scenario.estimated_time}
            </Badge>
            <Badge variant="outline" className="flex items-center gap-1">
              <BookOpen className="h-3 w-3" />
              {scenario.steps.length} steps
            </Badge>
          </div>
        </div>

        {/* Progress Bar */}
        <div className="space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Progress</span>
            <span className="font-medium">
              {completedSteps.length} / {scenario.steps.length} completed
            </span>
          </div>
          <Progress value={progressPercentage} className="h-2" />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[280px_1fr] gap-6">
        {/* Step Navigation Sidebar */}
        <div className="space-y-2">
          <h3 className="font-medium text-sm text-muted-foreground mb-3">Steps</h3>
          <div className="space-y-1">
            {scenario.steps.map((step, index) => {
              const isCompleted = completedSteps.includes(step.id)
              const isCurrent = index === currentStepIndex
              const Icon = stepTypeIcons[step.type] || Circle

              return (
                <button
                  key={step.id}
                  onClick={() => setCurrentStepIndex(index)}
                  className={cn(
                    "w-full flex items-center gap-3 px-3 py-2 rounded-lg text-left text-sm transition-colors",
                    isCurrent && "bg-primary/10 text-primary border border-primary/20",
                    !isCurrent && isCompleted && "text-muted-foreground hover:bg-muted",
                    !isCurrent && !isCompleted && "text-foreground hover:bg-muted"
                  )}
                >
                  <div className={cn(
                    "flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center text-xs",
                    isCompleted && "bg-green-100 text-green-600 dark:bg-green-900 dark:text-green-400",
                    !isCompleted && isCurrent && "bg-primary text-primary-foreground",
                    !isCompleted && !isCurrent && "bg-muted text-muted-foreground"
                  )}>
                    {isCompleted ? (
                      <Check className="h-3 w-3" />
                    ) : (
                      index + 1
                    )}
                  </div>
                  <span className="truncate flex-1">{step.title}</span>
                  <Icon className={cn(
                    "h-4 w-4 flex-shrink-0",
                    step.type === 'warning' && "text-yellow-500",
                    step.type === 'tip' && "text-blue-500"
                  )} />
                </button>
              )
            })}
          </div>
        </div>

        {/* Main Content */}
        <div className="space-y-6">
          <Card>
            <CardHeader>
              <div className="flex items-center gap-3">
                <div className={cn(
                  "p-2 rounded-lg",
                  currentStep.type === 'warning' && "bg-yellow-100 dark:bg-yellow-900",
                  currentStep.type === 'tip' && "bg-blue-100 dark:bg-blue-900",
                  currentStep.type === 'action' && "bg-primary/10",
                  currentStep.type === 'concept' && "bg-gray-100 dark:bg-gray-800",
                  currentStep.type === 'verification' && "bg-green-100 dark:bg-green-900"
                )}>
                  <StepIcon className={cn(
                    "h-5 w-5",
                    currentStep.type === 'warning' && "text-yellow-600 dark:text-yellow-400",
                    currentStep.type === 'tip' && "text-blue-600 dark:text-blue-400",
                    currentStep.type === 'action' && "text-primary",
                    currentStep.type === 'verification' && "text-green-600 dark:text-green-400"
                  )} />
                </div>
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <span className="text-muted-foreground text-sm font-normal">
                      Step {currentStepIndex + 1}
                    </span>
                    {currentStep.title}
                  </CardTitle>
                  <CardDescription className="capitalize">
                    {currentStep.type}
                  </CardDescription>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Content */}
              <div 
                className="prose prose-gray dark:prose-invert max-w-none"
                dangerouslySetInnerHTML={{ 
                  __html: currentStep.content
                    .replace(/\n\n/g, '</p><p>')
                    .replace(/^/, '<p>')
                    .replace(/$/, '</p>')
                }}
              />

              {/* Code Blocks */}
              {currentStep.code_blocks && currentStep.code_blocks.length > 0 && (
                <div className="space-y-4">
                  {currentStep.code_blocks.map((block, index) => (
                    <CodeBlock
                      key={index}
                      code={block.code}
                      language={block.language}
                      filename={block.filename}
                    />
                  ))}
                </div>
              )}

              {/* Warnings */}
              {currentStep.warnings && currentStep.warnings.length > 0 && (
                <div className="space-y-2">
                  {currentStep.warnings.map((warning, index) => (
                    <div 
                      key={index}
                      className="flex items-start gap-3 p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg"
                    >
                      <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400 flex-shrink-0 mt-0.5" />
                      <p className="text-sm text-yellow-800 dark:text-yellow-200">{warning}</p>
                    </div>
                  ))}
                </div>
              )}

              {/* Tips */}
              {currentStep.tips && currentStep.tips.length > 0 && (
                <div className="space-y-2">
                  {currentStep.tips.map((tip, index) => (
                    <div 
                      key={index}
                      className="flex items-start gap-3 p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg"
                    >
                      <Lightbulb className="h-5 w-5 text-blue-600 dark:text-blue-400 flex-shrink-0 mt-0.5" />
                      <p className="text-sm text-blue-800 dark:text-blue-200">{tip}</p>
                    </div>
                  ))}
                </div>
              )}

              {/* Expected Output */}
              {currentStep.expected_output && (
                <div className="space-y-2">
                  <h4 className="font-medium text-sm">Expected Output</h4>
                  <div className="p-4 bg-muted rounded-lg font-mono text-sm whitespace-pre-wrap">
                    {currentStep.expected_output}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Navigation */}
          <div className="flex items-center justify-between">
            <Button
              variant="outline"
              onClick={handlePrevious}
              disabled={currentStepIndex === 0}
            >
              <ChevronLeft className="mr-2 h-4 w-4" />
              Previous
            </Button>

            <div className="flex items-center gap-2">
              {!isCurrentStepComplete && (
                <Button 
                  variant="default" 
                  onClick={handleMarkComplete}
                  disabled={progressMutation.isPending}
                >
                  <Check className="mr-2 h-4 w-4" />
                  Mark Complete
                </Button>
              )}
              {isCurrentStepComplete && currentStepIndex === scenario.steps.length - 1 && (
                <Badge variant="success" className="px-4 py-2">
                  <Check className="mr-2 h-4 w-4" />
                  Guide Complete!
                </Badge>
              )}
            </div>

            <Button
              variant="outline"
              onClick={handleNext}
              disabled={currentStepIndex === scenario.steps.length - 1}
            >
              Next
              <ChevronRight className="ml-2 h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}
