const API_BASE_URL = '/api'

export interface CodeBlock {
  language: string
  code: string
  filename?: string
}

export interface Step {
  id: string
  title: string
  type: 'info' | 'command' | 'code' | 'config' | 'checkpoint' | 'warning' | 'tip' | 'action' | 'concept' | 'verification'
  content: string
  code_blocks?: CodeBlock[]
  expected_output?: string
  tips?: string[]
  warnings?: string[]
  duration_minutes?: number
  checkpoint_question?: string
}

export interface Scenario {
  id: string
  title: string
  description: string
  difficulty: 'beginner' | 'intermediate' | 'advanced'
  estimated_time: string
  estimated_duration_minutes: number
  prerequisites: string[]
  learning_outcomes: string[]
  tags: string[]
  steps: Step[]
  related_scenarios: string[]
  category: string
  order: number
}

export interface ScenarioSummary {
  id: string
  title: string
  description: string
  difficulty: 'beginner' | 'intermediate' | 'advanced'
  estimated_time: string
  estimated_duration_minutes: number
  tags: string[]
  category: string
  order: number
  step_count: number
}

export interface Category {
  id: string
  title: string
  description: string
  icon: string
  order: number
  scenarios: ScenarioSummary[]
}

export interface SearchResult {
  scenario_id: string
  scenario_title: string
  step_id?: string
  step_title?: string
  match_type: string
  snippet: string
  relevance: number
  difficulty: 'beginner' | 'intermediate' | 'advanced'
  matching_tags?: string[]
}

export interface UserProgress {
  scenario_id: string
  completed_steps: string[]
  started_at?: string
  last_accessed?: string
  completed: boolean
}

// API Functions

export async function fetchCategories(): Promise<Category[]> {
  const response = await fetch(`${API_BASE_URL}/scenarios/categories`)
  if (!response.ok) throw new Error('Failed to fetch categories')
  return response.json()
}

export async function fetchScenarios(filters?: {
  category?: string
  difficulty?: string
  tag?: string
}): Promise<ScenarioSummary[]> {
  const params = new URLSearchParams()
  if (filters?.category) params.append('category', filters.category)
  if (filters?.difficulty) params.append('difficulty', filters.difficulty)
  if (filters?.tag) params.append('tag', filters.tag)
  
  const url = `${API_BASE_URL}/scenarios/${params.toString() ? `?${params}` : ''}`
  const response = await fetch(url)
  if (!response.ok) throw new Error('Failed to fetch scenarios')
  return response.json()
}

export async function fetchScenario(scenarioId: string): Promise<Scenario> {
  const response = await fetch(`${API_BASE_URL}/scenarios/${scenarioId}`)
  if (!response.ok) throw new Error('Failed to fetch scenario')
  return response.json()
}

export async function searchScenarios(
  query: string,
  options?: { category?: string; limit?: number }
): Promise<SearchResult[]> {
  const params = new URLSearchParams({ q: query })
  if (options?.category) params.append('category', options.category)
  if (options?.limit) params.append('limit', options.limit.toString())
  
  const response = await fetch(`${API_BASE_URL}/search/?${params}`)
  if (!response.ok) throw new Error('Failed to search')
  return response.json()
}

export async function fetchProgress(scenarioId: string): Promise<UserProgress> {
  const response = await fetch(`${API_BASE_URL}/progress/${scenarioId}`)
  if (!response.ok) throw new Error('Failed to fetch progress')
  return response.json()
}

export async function updateProgress(
  scenarioId: string,
  stepId: string
): Promise<UserProgress> {
  const response = await fetch(`${API_BASE_URL}/progress/${scenarioId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ step_id: stepId, completed: true }),
  })
  if (!response.ok) throw new Error('Failed to update progress')
  return response.json()
}

export async function resetProgress(scenarioId: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/progress/${scenarioId}`, {
    method: 'DELETE',
  })
  if (!response.ok) throw new Error('Failed to reset progress')
}
