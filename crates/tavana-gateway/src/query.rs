//! Query parsing and resource estimation

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, GroupByExpr};
use std::collections::HashSet;
use tracing::instrument;

/// Parsed query information
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub sql: String,
    pub tables: Vec<String>,
    pub join_count: usize,
    pub has_aggregation: bool,
    pub has_window_function: bool,
    pub has_order_by: bool,
    pub has_distinct: bool,
    pub has_subquery: bool,
}

impl ParsedQuery {
    /// Parse a SQL query and extract metadata
    #[instrument]
    pub fn parse(sql: &str) -> anyhow::Result<Self> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        
        let mut tables = HashSet::new();
        let mut join_count = 0;
        let mut has_aggregation = false;
        let mut has_window_function = false;
        let mut has_order_by = false;
        let mut has_distinct = false;
        let mut has_subquery = false;
        
        for statement in &statements {
            if let Statement::Query(query) = statement {
                Self::analyze_query(
                    query,
                    &mut tables,
                    &mut join_count,
                    &mut has_aggregation,
                    &mut has_window_function,
                    &mut has_order_by,
                    &mut has_distinct,
                    &mut has_subquery,
                );
            }
        }
        
        Ok(Self {
            sql: sql.to_string(),
            tables: tables.into_iter().collect(),
            join_count,
            has_aggregation,
            has_window_function,
            has_order_by,
            has_distinct,
            has_subquery,
        })
    }

    fn analyze_query(
        query: &Query,
        tables: &mut HashSet<String>,
        join_count: &mut usize,
        has_aggregation: &mut bool,
        has_window_function: &mut bool,
        has_order_by: &mut bool,
        has_distinct: &mut bool,
        has_subquery: &mut bool,
    ) {
        // Check for ORDER BY
        if let Some(ref order_by) = query.order_by {
            if !order_by.exprs.is_empty() {
                *has_order_by = true;
            }
        }

        // Analyze the query body
        if let SetExpr::Select(select) = query.body.as_ref() {
            // Check for DISTINCT
            if select.distinct.is_some() {
                *has_distinct = true;
            }

            // Extract tables from FROM clause
            for table_with_joins in &select.from {
                Self::extract_table(&table_with_joins.relation, tables, has_subquery);
                
                // Count joins
                *join_count += table_with_joins.joins.len();
                
                for join in &table_with_joins.joins {
                    Self::extract_table(&join.relation, tables, has_subquery);
                }
            }

            // Check for aggregations and window functions in projection
            for item in &select.projection {
                let item_str = format!("{}", item);
                if item_str.contains("COUNT(") || 
                   item_str.contains("SUM(") || 
                   item_str.contains("AVG(") ||
                   item_str.contains("MIN(") ||
                   item_str.contains("MAX(") ||
                   item_str.contains("GROUP BY") {
                    *has_aggregation = true;
                }
                if item_str.contains("OVER(") || item_str.contains("OVER (") {
                    *has_window_function = true;
                }
            }

            // Check GROUP BY
            match &select.group_by {
                GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                    *has_aggregation = true;
                }
                GroupByExpr::All(_) => {
                    *has_aggregation = true;
                }
                _ => {}
            }
        }
    }

    fn extract_table(
        table_factor: &TableFactor,
        tables: &mut HashSet<String>,
        has_subquery: &mut bool,
    ) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                tables.insert(name.to_string());
            }
            TableFactor::Derived { .. } => {
                *has_subquery = true;
                // Recursively analyze subquery
                // TODO: Implement recursive analysis
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                Self::extract_table(&table_with_joins.relation, tables, has_subquery);
            }
            _ => {}
        }
    }

    /// Compute a fingerprint for query similarity matching
    pub fn fingerprint(&self) -> String {
        // Sort tables for consistent hashing
        let mut sorted_tables = self.tables.clone();
        sorted_tables.sort();
        
        let features = format!(
            "tables:{};joins:{};agg:{};window:{};order:{};distinct:{}",
            sorted_tables.join(","),
            self.join_count,
            self.has_aggregation,
            self.has_window_function,
            self.has_order_by,
            self.has_distinct,
        );
        
        let hash = blake3::hash(features.as_bytes());
        hash.to_hex().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_query() {
        let query = ParsedQuery::parse("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(query.tables, vec!["users"]);
        assert_eq!(query.join_count, 0);
        assert!(!query.has_aggregation);
    }

    #[test]
    fn test_join_query() {
        let query = ParsedQuery::parse(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        ).unwrap();
        assert_eq!(query.join_count, 1);
        assert!(query.tables.contains(&"users".to_string()) || query.tables.contains(&"u".to_string()));
    }

    #[test]
    fn test_aggregation_query() {
        let query = ParsedQuery::parse(
            "SELECT COUNT(*), SUM(amount) FROM orders GROUP BY user_id"
        ).unwrap();
        assert!(query.has_aggregation);
    }
}

