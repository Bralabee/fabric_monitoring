"""
Neo4j Client for Lineage Explorer
---------------------------------
Connection management and query execution for Neo4j graph database.
"""

import logging
import os
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Neo4jClient:
    """
    Neo4j database client with connection pooling and query helpers.
    
    Usage:
        client = Neo4jClient()
        client.connect()
        results = client.run_query("MATCH (n) RETURN n LIMIT 10")
        client.close()
    """
    
    def __init__(
        self,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "neo4j"
    ):
        """
        Initialize Neo4j client.
        
        Args:
            uri: Neo4j bolt URI (default: bolt://localhost:7687)
            username: Neo4j username (default: neo4j)
            password: Neo4j password (default from NEO4J_PASSWORD env var)
            database: Database name (default: neo4j)
        """
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.username = username or os.getenv("NEO4J_USERNAME", "neo4j")
        # SECURITY: No default password - must be set via env var or parameter
        self.password = password or os.getenv("NEO4J_PASSWORD")
        if not self.password:
            raise ValueError(
                "NEO4J_PASSWORD environment variable is required. "
                "Set it in your .env file or pass password parameter explicitly."
            )
        self.database = database
        self._driver = None
        
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._driver is not None
    
    def connect(self) -> "Neo4jClient":
        """
        Establish connection to Neo4j.
        
        Returns:
            self for method chaining
            
        Raises:
            ImportError: If neo4j package is not installed
            Exception: If connection fails
        """
        try:
            from neo4j import GraphDatabase
        except ImportError:
            raise ImportError(
                "neo4j package not installed. "
                "Install with: pip install neo4j"
            )
        
        logger.info(f"Connecting to Neo4j at {self.uri}")
        self._driver = GraphDatabase.driver(
            self.uri,
            auth=(self.username, self.password)
        )
        
        # Verify connection
        self._driver.verify_connectivity()
        logger.info("Neo4j connection established")
        
        return self
    
    def close(self):
        """Close the database connection."""
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed")
    
    @contextmanager
    def session(self):
        """Get a database session context manager."""
        if not self._driver:
            raise RuntimeError("Not connected. Call connect() first.")
        
        session = self._driver.session(database=self.database)
        try:
            yield session
        finally:
            session.close()
    
    def run_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return results.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            List of result records as dictionaries
        """
        with self.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def run_query_single(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a Cypher query and return single result.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            Single result record or None
        """
        results = self.run_query(query, parameters)
        return results[0] if results else None
    
    def run_write_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a write query and return summary.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            Summary with nodes_created, relationships_created, etc.
        """
        with self.session() as session:
            result = session.run(query, parameters or {})
            summary = result.consume()
            return {
                "nodes_created": summary.counters.nodes_created,
                "nodes_deleted": summary.counters.nodes_deleted,
                "relationships_created": summary.counters.relationships_created,
                "relationships_deleted": summary.counters.relationships_deleted,
                "properties_set": summary.counters.properties_set,
                "labels_added": summary.counters.labels_added,
            }
    
    def run_batch_write(
        self,
        query: str,
        batch_data: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> Dict[str, int]:
        """
        Execute a write query in batches for large datasets.
        
        Args:
            query: Cypher query with $batch parameter
            batch_data: List of records to process
            batch_size: Records per batch
            
        Returns:
            Aggregate summary of all batches
        """
        totals = {
            "nodes_created": 0,
            "relationships_created": 0,
            "properties_set": 0,
            "batches_processed": 0
        }
        
        for i in range(0, len(batch_data), batch_size):
            batch = batch_data[i:i + batch_size]
            summary = self.run_write_query(query, {"batch": batch})
            
            totals["nodes_created"] += summary["nodes_created"]
            totals["relationships_created"] += summary["relationships_created"]
            totals["properties_set"] += summary["properties_set"]
            totals["batches_processed"] += 1
            
            logger.debug(f"Batch {totals['batches_processed']}: {len(batch)} records")
        
        return totals
    
    def clear_database(self) -> Dict[str, Any]:
        """
        Clear all nodes and relationships from the database.
        
        Returns:
            Summary of deleted items
        """
        logger.warning("Clearing all data from Neo4j database")
        return self.run_write_query("MATCH (n) DETACH DELETE n")
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get the database schema information.
        
        Returns:
            Dict with node labels, relationship types, and property keys
        """
        labels = self.run_query("CALL db.labels()")
        rel_types = self.run_query("CALL db.relationshipTypes()")
        
        return {
            "node_labels": [r["label"] for r in labels],
            "relationship_types": [r["relationshipType"] for r in rel_types]
        }
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get database statistics.
        
        Returns:
            Dict with node and relationship counts
        """
        result = self.run_query_single("""
            MATCH (n)
            WITH count(n) as nodes
            MATCH ()-[r]->()
            RETURN nodes, count(r) as relationships
        """)
        
        return {
            "nodes": result.get("nodes", 0) if result else 0,
            "relationships": result.get("relationships", 0) if result else 0
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the database.
        
        Returns:
            Health status including connectivity and stats
        """
        try:
            if not self._driver:
                return {"healthy": False, "error": "Not connected"}
            
            self._driver.verify_connectivity()
            stats = self.get_stats()
            
            return {
                "healthy": True,
                "uri": self.uri,
                "database": self.database,
                "nodes": stats["nodes"],
                "relationships": stats["relationships"]
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
