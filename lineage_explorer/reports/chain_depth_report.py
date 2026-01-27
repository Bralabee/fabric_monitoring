"""
Chain Depth Report Generator

Generates comprehensive reports analyzing dependency chain depths
in the Fabric lineage graph. Outputs markdown with Mermaid diagrams.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class ChainDepthReport:
    """Container for chain depth analysis results."""
    generated_at: str
    stats: Dict[str, Any]
    deepest_chains: List[Dict[str, Any]]
    items_by_depth: List[Dict[str, Any]]
    
    def to_markdown(self) -> str:
        """Generate markdown report with Mermaid diagram."""
        lines = [
            "# Chain Depth Analysis Report",
            f"",
            f"> Generated: {self.generated_at}",
            "",
            "## Summary Statistics",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Maximum Chain Depth | {self.stats.get('max_depth', 0)} |",
            f"| Average Chain Depth | {self.stats.get('avg_depth', 0):.2f} |",
            f"| Total Chains (≥2 hops) | {self.stats.get('chain_count', 0)} |",
            "",
        ]
        
        # Depth distribution table
        dist = self.stats.get('depth_distribution', [])
        if dist:
            lines.extend([
                "### Depth Distribution",
                "",
                "| Depth | Count |",
                "|-------|-------|",
            ])
            for item in dist:
                lines.append(f"| {item.get('depth', 0)} | {item.get('count', 0)} |")
            lines.append("")
        
        # Deepest chains section
        if self.deepest_chains:
            lines.extend([
                "## Deepest Chains (Top 10)",
                "",
            ])
            for i, chain in enumerate(self.deepest_chains[:10], 1):
                depth = chain.get('depth', 0)
                terminal = chain.get('terminal_name', 'Unknown')
                origin = chain.get('origin_name', 'Unknown')
                chain_names = chain.get('chain_names', [])
                
                lines.extend([
                    f"### Chain {i}: Depth {depth}",
                    f"",
                    f"**Terminal:** {terminal} → **Origin:** {origin}",
                    f"",
                    f"**Path:** {' → '.join(chain_names) if chain_names else 'N/A'}",
                    "",
                ])
            
            # Mermaid diagram for deepest chain
            if self.deepest_chains:
                deepest = self.deepest_chains[0]
                chain_names = deepest.get('chain_names', [])
                if len(chain_names) >= 2:
                    lines.extend([
                        "### Deepest Chain Visualization",
                        "",
                        "```mermaid",
                        "flowchart LR",
                    ])
                    for i, name in enumerate(chain_names):
                        safe_name = name.replace('"', "'").replace('[', '(').replace(']', ')')
                        node_id = f"N{i}"
                        lines.append(f'    {node_id}["{safe_name}"]')
                        if i > 0:
                            lines.append(f"    N{i-1} --> {node_id}")
                    lines.extend(["```", ""])
        
        # Items by total chain depth
        if self.items_by_depth:
            lines.extend([
                "## Items by Chain Depth",
                "",
                "| Item | Type | Upstream | Downstream | Total |",
                "|------|------|----------|------------|-------|",
            ])
            for item in self.items_by_depth[:20]:
                lines.append(
                    f"| {item.get('item_name', 'N/A')} | {item.get('item_type', 'N/A')} | "
                    f"{item.get('upstream_depth', 0)} | {item.get('downstream_depth', 0)} | "
                    f"{item.get('total_chain_depth', 0)} |"
                )
            lines.append("")
        
        return "\n".join(lines)


class ChainDepthReporter:
    """Generates chain depth analysis reports from Neo4j queries."""
    
    def __init__(self, queries_client):
        """
        Initialize with a LineageQueries client.
        
        Args:
            queries_client: Instance of LineageQueries with Neo4j connection
        """
        self.queries = queries_client
    
    def generate_report(self) -> ChainDepthReport:
        """
        Generate a comprehensive chain depth report.
        
        Returns:
            ChainDepthReport with all analysis data
        """
        logger.info("Generating chain depth report...")
        
        stats = self.queries.get_chain_depth_stats()
        deep_chains = self.queries.get_deep_chains(min_depth=3)
        node_depths = self.queries.get_node_chain_depths()
        
        return ChainDepthReport(
            generated_at=datetime.now().isoformat(),
            stats=stats,
            deepest_chains=deep_chains,
            items_by_depth=node_depths
        )
    
    def save_report(
        self,
        output_path: Optional[str] = None,
        format: str = "markdown"
    ) -> str:
        """
        Generate and save report to file.
        
        Args:
            output_path: Path to save report (default: exports/reports/)
            format: Output format ('markdown')
            
        Returns:
            Path to saved report file
        """
        report = self.generate_report()
        
        if output_path is None:
            output_dir = Path("exports/reports")
            output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = output_dir / f"chain_depth_report_{timestamp}.md"
        else:
            output_path = Path(output_path)
        
        content = report.to_markdown()
        output_path.write_text(content)
        logger.info(f"Report saved to {output_path}")
        
        return str(output_path)


def generate_chain_depth_report(neo4j_queries) -> str:
    """
    Convenience function to generate and return markdown report.
    
    Args:
        neo4j_queries: LineageQueries instance
        
    Returns:
        Markdown report content as string
    """
    reporter = ChainDepthReporter(neo4j_queries)
    report = reporter.generate_report()
    return report.to_markdown()
