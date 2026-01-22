"""
Visualize Fabric Lineage Topology
---------------------------------
Generates a "Deep Dive" Lineage Dashboard visualizing the complex topology 
of Fabric Lineage data.

Visualizations:
1. Topology Network Graph (Force-Directed)
2. Sankey Diagram (Flow Analysis)
3. Inverted Treemap (Usage/Reverse Lineage)

Usage:
    python src/usf_fabric_monitoring/scripts/visualize_lineage.py --input exports/lineage/mirrored_lineage_latest.csv
    python src/usf_fabric_monitoring/scripts/visualize_lineage.py --find-latest
"""
import os
import sys
import argparse
import logging
import pandas as pd
import numpy as np
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def truncate_label(text, width=30):
    """Truncate text for graph labels."""
    if not isinstance(text, str):
        return str(text)
    return (text[:width] + '...') if len(text) > width else text

def load_data(file_path):
    """Load and clean lineage data."""
    logger.info(f"Loading data from {file_path}")
    df = pd.read_csv(file_path)
    
    # Standardize column names if needed or ensure they exist
    required_columns = ['Workspace Name', 'Item Name', 'Item Type', 'Source Type', 'Source Connection']
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        # Try to work with what we have or map common variations (e.g. from older scripts)
        logger.warning(f"Missing columns: {missing}. Data functionality may be limited.")
    
    # Fill NaNs
    df['Workspace Name'] = df['Workspace Name'].fillna('Unknown Workspace')
    df['Item Name'] = df['Item Name'].fillna('Unnamed Item')
    df['Item Type'] = df['Item Type'].fillna('Unknown Type')
    df['Source Type'] = df['Source Type'].fillna('Unknown Source')
    df['Source Connection'] = df['Source Connection'].fillna('Unknown Connection')
    
    # Create a unique Source ID for grouping
    df['Source ID'] = df['Source Type'] + ":" + df['Source Connection']
    
    return df

def create_topology_graph(df):
    """
    Create a Force-Directed Network Graph using NetworkX and Plotly.
    Visualizes Workspaces -> Items -> Sources clustering.
    """
    logger.info("Building Topology Network Graph...")
    
    G = nx.Graph()
    
    # Add Nodes and Edges
    for _, row in df.iterrows():
        ws = row['Workspace Name']
        item = row['Item Name']
        item_type = row['Item Type']
        source_id = row['Source ID']
        source_type = row['Source Type']
        source_conn = row['Source Connection']
        
        # Workspace Node (Level 1)
        if not G.has_node(ws):
            G.add_node(ws, node_type='Workspace', size=25, color='#3b82f6', symbol='square', label=ws)
            
        # Item Node (Level 2)
        # Make Item unique per workspace to avoid collisions if items share names in diff workspaces
        # But if we want to see item reuse? Usually items are scoped to WS. 
        # Using item + ws hash for uniqueness in graph
        item_node_id = f"ITEM:{ws}:{item}"
        if not G.has_node(item_node_id):
            G.add_node(item_node_id, node_type='Item', size=15, color='#10b981', symbol='circle', label=f"{item} ({item_type})")
        
        G.add_edge(ws, item_node_id, weight=2)
        
        # Source Node (Level 3) - Shared across universe
        # Only add source if it's not internal/unknown/none
        if source_type not in ['Unknown', 'None', 'Unknown Source'] and source_conn not in ['Unknown Connection']:
            if not G.has_node(source_id):
                # Calculate degree later for size
                G.add_node(source_id, node_type='Source', size=10, color='#ef4444', symbol='diamond', label=f"{source_type}: {truncate_label(source_conn)}")
            
            G.add_edge(item_node_id, source_id, weight=1)

    # Compute Layout
    logger.info(f"Computing layout for {G.number_of_nodes()} nodes and {G.number_of_edges()} edges...")
    # k parameter controls spacing. smaller k = tighter.
    pos = nx.spring_layout(G, k=0.15, iterations=50, seed=42)
    # Alternatively use kamada_kawai for better structure on small graphs
    # pos = nx.kamada_kawai_layout(G)

    # Extract Node Data for Plotting
    node_x = []
    node_y = []
    node_text = []
    node_color = []
    node_size = []
    node_symbol = []
    
    # Adjust Source Node Size by Degree (Centrality)
    degrees = dict(G.degree())
    max_degree = max(degrees.values()) if degrees else 1
    
    for node in G.nodes(data=True):
        x, y = pos[node[0]]
        node_x.append(x)
        node_y.append(y)
        
        data = node[1]
        n_type = data.get('node_type', 'Unknown')
        
        # Dynamic sizing for Sources based on connections
        size = data.get('size', 10)
        if n_type == 'Source':
            deg = degrees.get(node[0], 1)
            # Scale factor
            size = 10 + (deg * 1.5)
            # Cap size
            size = min(size, 40)
            
        node_size.append(size)
        node_color.append(data.get('color', '#888'))
        node_symbol.append(data.get('symbol', 'circle'))
        
        # Hover text
        hover = f"<b>{data.get('label')}</b><br>Type: {n_type}"
        if n_type == 'Source':
            hover += f"<br>Connections: {degrees.get(node[0], 0)}"
        node_text.append(hover)

    # Extract Edge Data for Plotting
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # Create Traces
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#555'),
        hoverinfo='none',
        mode='lines')

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=False,
            color=node_color,
            size=node_size,
            symbol=node_symbol,
            line_width=1,
            line_color='#fff'))

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title=dict(text='Fabric Topology Network', font=dict(size=16)),
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20,l=5,r=5,t=40),
                        paper_bgcolor='#0f172a', # Dark Navy
                        plot_bgcolor='#0f172a',
                        font=dict(color='#e2e8f0'),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )
    return fig

def create_sankey_diagram(df):
    """
    Create a Sankey Diagram: Workspace -> Item Type -> Source Type
    """
    logger.info("Building Sankey Diagram...")
    
    # -- Step 1: Aggregate Data --
    # Level 1 to 2: Workspace -> Item Type
    flow1 = df.groupby(['Workspace Name', 'Item Type']).size().reset_index(name='count')
    
    # Level 2 to 3: Item Type -> Source Type
    # Filters out Unknown sources for cleaner flow
    df_valid_source = df[~df['Source Type'].isin(['Unknown Source', 'None'])]
    if df_valid_source.empty:
        # Fallback if no sources
        df_valid_source = df
        
    flow2 = df_valid_source.groupby(['Item Type', 'Source Type']).size().reset_index(name='count')
    
    # -- Step 2: Create Index Map --
    all_nodes = list(pd.concat([
        flow1['Workspace Name'], 
        flow1['Item Type'], 
        flow2['Source Type']
    ]).unique())
    
    node_map = {node: i for i, node in enumerate(all_nodes)}
    
    # -- Step 3: Build Links --
    sources = []
    targets = []
    values = []
    
    # Link WS -> Item Type
    for _, row in flow1.iterrows():
        try:
            sources.append(node_map[row['Workspace Name']])
            targets.append(node_map[row['Item Type']])
            values.append(row['count'])
        except KeyError as e:
            logger.warning(f"Sankey node missing key: {e}")
            continue
        
    # Link Item Type -> Source Type
    for _, row in flow2.iterrows():
        try:
            sources.append(node_map[row['Item Type']])
            targets.append(node_map[row['Source Type']])
            values.append(row['count'])
        except KeyError as e:
            logger.warning(f"Sankey node missing key: {e}")
            continue
    
    # -- Step 4: Visual --
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=all_nodes,
            color="#3b82f6"
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color='rgba(148, 163, 184, 0.4)' # Navy-400 with opacity
        )
    )])
    
    fig.update_layout(
        title_text="Data Flow: Workspace → Item Type → Source",
        font_size=12,
        paper_bgcolor='#0f172a',
        plot_bgcolor='#0f172a',
        font=dict(color='#e2e8f0')
    )
    return fig

def create_inverted_treemap(df):
    """
    Create Inverted Treemap: Source Type -> Connection -> Workspace
    """
    logger.info("Building Inverted Treemap...")
    
    # Filter for meaningful sources
    df_tree = df[~df['Source Type'].isin(['Unknown Source', 'None'])].copy()
    
    if df_tree.empty:
        # Create a dummy entry if empty to avoid error
        df_tree = pd.DataFrame([{
            'Source Type': 'No External Sources', 
            'Source Connection': 'N/A', 
            'Workspace Name': 'N/A',
            'count': 1
        }])
    else:
        df_tree['count'] = 1
    
    fig = px.treemap(
        df_tree, 
        path=[px.Constant("All Sources"), 'Source Type', 'Source Connection', 'Workspace Name'], 
        title='Reverse Lineage: Who uses what?',
        color_discrete_sequence=px.colors.sequential.Teal
    )
    
    fig.update_layout(
        margin=dict(t=50, l=25, r=25, b=25),
        paper_bgcolor='#0f172a',
        plot_bgcolor='#0f172a',
        font=dict(color='#e2e8f0')
    )
    return fig

def generate_dashboard_html(figs, output_path):
    """
    Combine figures into a single HTML dashboard.
    """
    logger.info(f"Generating dashboard at {output_path}...")
    
    # Create HTML strings for each figure
    # Note: Using full_html=False to embed in our custom template
    divs = [fig.to_html(full_html=False, include_plotlyjs=False) for fig in figs]
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Fabric Deep Dive Lineage</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
            .container {{ max-width: 1400px; margin: 0 auto; }}
            .card {{ background-color: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 30px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }}
            h1 {{ color: #ffffff; border-bottom: 2px solid #3b82f6; padding-bottom: 10px; margin-bottom: 30px; }}
            h2 {{ color: #3b82f6; margin-top: 0; }}
            p {{ color: #94a3b8; line-height: 1.6; margin-bottom: 20px; }}
            .metric {{ font-size: 24px; font-weight: bold; color: #10b981; }}
            .header-meta {{ font-size: 0.9em; color: #64748b; margin-top: -20px; margin-bottom: 40px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🕸️ Fabric Lineage Deep Dive</h1>
            <div class="header-meta">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
            
            <div class="card">
                <h2>1. Global Topology Network</h2>
                <p><strong>Use this to find clustering and "Gravity" in your architecture.</strong><br>
                • 🟦 <strong>Blue Squares:</strong> Workspaces<br>
                • 🟩 <strong>Green Circles:</strong> Items (Lakehouses, Warehouses etc.)<br>
                • 🟥 <strong>Red Diamonds:</strong> External Sources (ADLS, Snowflake, SQL)<br>
                If multiple workspaces connect to the same Red Diamond, they will cluster together, revealing data dependencies and potential data hubs.</p>
                {divs[0]}
            </div>
            
            <div class="card">
                <h2>2. System Flow (Sankey)</h2>
                <p><strong>Use this to understand architectural patterns.</strong> Trace how data flows from Workspaces through specific Item Types 
                out to External Sources. Thick bands indicate high-volume patterns.</p>
                {divs[1]}
            </div>
            
            <div class="card">
                <h2>3. Source Usage (Reverse Lineage)</h2>
                <p><strong>Use this for security and impact analysis.</strong> Drill down from Source Type → Connection to see which Workspaces 
                are consuming specific external resources (e.g., Who is reading from "StorageAccountA"?).</p>
                {divs[2]}
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"✅ Dashboard saved to {output_path}")
    print(f"Dashboard generated: {output_path}")

def find_latest_lineage_file(base_dir="exports/lineage"):
    """Find the most recent lineage CSV."""
    p = Path(base_dir)
    if not p.exists():
        return None
    files = list(p.glob("mirrored_lineage_*.csv"))
    if not files:
        # Fallback to general csv if specific prefix absent or changed
        files = list(p.glob("*.csv"))
    
    if not files:
        return None
        
    return max(files, key=os.path.getmtime)

def main():
    parser = argparse.ArgumentParser(description="Generate Fabric Lineage Dashboard")
    parser.add_argument("--input", "-i", type=str, help="Path to input CSV file")
    parser.add_argument("--output", "-o", type=str, default="exports/lineage/lineage_dashboard.html", help="Path for output HTML")
    parser.add_argument("--find-latest", action="store_true", help="Automatically use the latest file in exports/lineage")
    
    args = parser.parse_args()
    
    input_path = args.input
    
    # Auto-find logic
    if args.find_latest or (not input_path):
        latest = find_latest_lineage_file()
        if latest:
            input_path = str(latest)
            logger.info(f"Found latest file: {input_path}")
        elif not input_path:
            logger.error("No input file provided and no lineage files found in exports/lineage.")
            logger.error("Run 'extract_lineage.py' first or specify --input.")
            sys.exit(1)

    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
        
    # Ensure output dir exists
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    
    # Pipeline
    try:
        df = load_data(input_path)
        
        if df.empty:
            logger.error("Input CSV is empty.")
            sys.exit(1)
            
        fig_topo = create_topology_graph(df)
        fig_sankey = create_sankey_diagram(df)
        fig_tree = create_inverted_treemap(df)
        
        generate_dashboard_html([fig_topo, fig_sankey, fig_tree], args.output)
        
    except Exception as e:
        logger.error(f"Failed to generate dashboard: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
