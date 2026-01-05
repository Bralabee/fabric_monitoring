import { useState } from 'react'
import { Check, Copy } from 'lucide-react'
import { Button } from './ui/button'
import { cn } from '@/lib/utils'

interface CodeBlockProps {
  code: string
  language: string
  filename?: string
  className?: string
}

// Basic syntax highlighting for common patterns
function highlightCode(code: string, language: string): string {
  // Escape HTML first
  let highlighted = code
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')

  if (language === 'python') {
    // Keywords
    highlighted = highlighted.replace(
      /\b(import|from|as|def|class|return|if|else|elif|for|while|try|except|finally|with|yield|lambda|pass|break|continue|raise|True|False|None|and|or|not|in|is)\b/g,
      '<span class="text-purple-600 dark:text-purple-400">$1</span>'
    )
    // Functions
    highlighted = highlighted.replace(
      /\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/g,
      '<span class="text-blue-600 dark:text-blue-400">$1</span>('
    )
    // Strings
    highlighted = highlighted.replace(
      /(["'])((?:\\.|(?!\1)[^\\])*)\1/g,
      '<span class="text-green-600 dark:text-green-400">$1$2$1</span>'
    )
    // Comments
    highlighted = highlighted.replace(
      /(#.*$)/gm,
      '<span class="text-gray-500 dark:text-gray-400 italic">$1</span>'
    )
    // Numbers
    highlighted = highlighted.replace(
      /\b(\d+\.?\d*)\b/g,
      '<span class="text-orange-600 dark:text-orange-400">$1</span>'
    )
  } else if (language === 'bash' || language === 'shell') {
    // Commands
    highlighted = highlighted.replace(
      /^(\s*)(make|conda|python|pip|git|cd|ls|cat|echo|export|source)\b/gm,
      '$1<span class="text-green-600 dark:text-green-400">$2</span>'
    )
    // Variables
    highlighted = highlighted.replace(
      /(\$[A-Z_][A-Z0-9_]*)/g,
      '<span class="text-yellow-600 dark:text-yellow-400">$1</span>'
    )
    // Flags
    highlighted = highlighted.replace(
      /(\s)(--?[a-zA-Z][a-zA-Z0-9-]*)/g,
      '$1<span class="text-blue-600 dark:text-blue-400">$2</span>'
    )
    // Comments
    highlighted = highlighted.replace(
      /(#.*$)/gm,
      '<span class="text-gray-500 dark:text-gray-400 italic">$1</span>'
    )
    // Strings
    highlighted = highlighted.replace(
      /(["'])((?:\\.|(?!\1)[^\\])*)\1/g,
      '<span class="text-orange-600 dark:text-orange-400">$1$2$1</span>'
    )
  } else if (language === 'json' || language === 'yaml') {
    // Keys
    highlighted = highlighted.replace(
      /^(\s*)("[^"]+"|[a-zA-Z_][a-zA-Z0-9_]*)(:)/gm,
      '$1<span class="text-blue-600 dark:text-blue-400">$2</span>$3'
    )
    // Strings
    highlighted = highlighted.replace(
      /(:\s*)(["'])((?:\\.|(?!\2)[^\\])*)\2/g,
      '$1<span class="text-green-600 dark:text-green-400">$2$3$2</span>'
    )
    // Numbers
    highlighted = highlighted.replace(
      /(:\s*)(\d+\.?\d*)/g,
      '$1<span class="text-orange-600 dark:text-orange-400">$2</span>'
    )
    // Booleans
    highlighted = highlighted.replace(
      /(:\s*)(true|false|null)/gi,
      '$1<span class="text-purple-600 dark:text-purple-400">$2</span>'
    )
    // Comments (YAML)
    highlighted = highlighted.replace(
      /(#.*$)/gm,
      '<span class="text-gray-500 dark:text-gray-400 italic">$1</span>'
    )
  }

  return highlighted
}

export function CodeBlock({ code, language, filename, className }: CodeBlockProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const highlightedCode = highlightCode(code, language)

  return (
    <div className={cn("relative group rounded-lg border bg-muted/50", className)}>
      {/* Header */}
      <div className="flex items-center justify-between border-b px-4 py-2 bg-muted/30 rounded-t-lg">
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase">
            {language}
          </span>
          {filename && (
            <>
              <span className="text-muted-foreground">·</span>
              <span className="text-xs text-muted-foreground font-mono">
                {filename}
              </span>
            </>
          )}
        </div>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 opacity-0 group-hover:opacity-100 transition-opacity"
          onClick={handleCopy}
        >
          {copied ? (
            <Check className="h-4 w-4 text-green-500" />
          ) : (
            <Copy className="h-4 w-4" />
          )}
        </Button>
      </div>

      {/* Code content */}
      <pre className="overflow-x-auto p-4 text-sm">
        <code
          className="font-mono"
          dangerouslySetInnerHTML={{ __html: highlightedCode }}
        />
      </pre>
    </div>
  )
}
