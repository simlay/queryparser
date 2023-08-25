use serde::Deserialize;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::collections::HashMap;
#[derive(Debug, Deserialize, PartialEq)]
pub struct FullColumn {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
}
#[derive(Debug, Deserialize, PartialEq)]
pub struct QueryAnalytics {
    metadata: Vec<FullColumn>,
    pub dependency_map: HashMap<String, (Vec<String>, bool)>,
}

impl QueryAnalytics {
    fn add_dependency(
        &mut self,
        dependent_column: String,
        upstream_identifier: String,
        new_opacity: bool,
    ) {
        if let Some((dependent_column, opacity)) = self.dependency_map.get_mut(&dependent_column) {
            // TODO: make this a hashset.
            if !dependent_column.contains(&upstream_identifier) {
                dependent_column.push(upstream_identifier);
                *opacity = new_opacity;
            }
        } else {
            self.dependency_map.insert(
                dependent_column.clone(),
                (vec![upstream_identifier], new_opacity),
            );
        }
    }

    fn search_for_columns_in_tables(
        &self,
        from: Vec<TableWithJoins>,
        column_name: String,
        table_name: Option<String>,
    ) -> Option<String> {
        let mut needle: Option<String> = None;
        if let Some(table_name) = table_name {
            for meta in &self.metadata {
                if column_name == meta.column_name && table_name == meta.table_name {
                    needle = Some(format!(
                        "{}.{}.{}.{}",
                        meta.database_name, meta.schema_name, meta.table_name, meta.column_name
                    ));
                    return needle;
                }
            }
        } else {
            for TableWithJoins { relation, .. } in &from {
                match relation {
                    TableFactor::Table { name, .. } => {
                        let full_name = name.0.clone();
                        let potential_table_name: Option<String> =
                            full_name.last().map(|v| v.value.clone());
                        let schema_name: Option<String> = if full_name.len() > 1 {
                            full_name.first().map(|v| v.value.clone())
                        } else {
                            None
                        };
                        let potential_table_name = potential_table_name
                            .expect("Could not retrieve table name out of struct");
                        for meta in &self.metadata {
                            if column_name == meta.column_name
                                && ((table_name.is_none()
                                    && meta.table_name == potential_table_name)
                                    || (table_name.is_some()
                                        && Some(meta.table_name.clone()) == table_name))
                                && ((schema_name.is_some()
                                    && Some(meta.schema_name.clone()) == schema_name)
                                    || schema_name.is_none())
                            {
                                needle = Some(format!(
                                    "{}.{}.{}.{}",
                                    meta.database_name,
                                    meta.schema_name,
                                    meta.table_name,
                                    meta.column_name
                                ));
                                return needle;
                            }
                        }
                    }
                    TableFactor::Derived { subquery, .. } => {
                        let expr = *subquery.body.clone();
                        let needle = self.search_for_columns_in_expr(
                            expr,
                            column_name.clone(),
                            table_name.clone(),
                        );
                        if needle.is_some() {
                            return needle;
                        }
                    }
                    val => {
                        println!("Skipping this relation: {val:?}");
                        continue;
                    }
                }
            }
        }
        needle
    }
    fn search_for_columns_in_expr(
        &self,
        set_expr: SetExpr,
        column_name: String,
        table_name: Option<String>,
    ) -> Option<String> {
        match set_expr {
            SetExpr::Select(select) => {
                return self.search_for_columns_in_tables(select.from, column_name, table_name);
            }
            SetExpr::Query(_) => todo!(),
            SetExpr::SetOperation { left, right, .. } => {
                let out = self.search_for_columns_in_expr(
                    *left.clone(),
                    column_name.clone(),
                    table_name.clone(),
                );
                if out.is_some() {
                    return out;
                }
                let out = self.search_for_columns_in_expr(*right.clone(), column_name, table_name);
                if out.is_some() {
                    return out;
                }
            }
            SetExpr::Values(_) => todo!(),
            SetExpr::Insert(_) => todo!(),
            SetExpr::Update(_) => todo!(),
            SetExpr::Table(_) => todo!(),
        }
        None
    }
    pub fn search_for_col_and_add(
        &mut self,
        from: Vec<TableWithJoins>,
        column_name: String,
        alias: Option<String>,
        table_name: Option<String>,
        opaque: bool,
    ) {
        if let Some(full_identifier) =
            self.search_for_columns_in_tables(from, column_name.clone(), table_name.clone())
        {
            if let Some(alias) = alias {
                self.add_dependency(alias, full_identifier, opaque);
            } else {
                self.add_dependency(column_name, full_identifier, opaque);
            }
        }
    }

    fn traverse_set_expr(&mut self, set_expr: SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                let mut output_col = String::new();
                let mut dependency_alias = None;
                let mut out_col_table: Option<String> = None;
                let select = *select;
                for projection in &select.projection {
                    match projection {
                        SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                            output_col = id.value.clone();
                        }
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ids)) => {
                            if ids.len() > 1 {
                                out_col_table = ids.first().map(|v| v.value.clone());
                            }
                            let id = ids
                                .last()
                                .expect("Failed to get identifier from compound identifier");
                            output_col = id.value.clone();
                        }
                        SelectItem::UnnamedExpr(_) => {}
                        SelectItem::ExprWithAlias { expr, alias } => {
                            dependency_alias = Some(alias.value.clone());
                            match expr {
                                Expr::Function(function) => {
                                    // TODO: Use this function name for opaque stuff.
                                    let function_name = function
                                        .name
                                        .0
                                        .clone()
                                        .first()
                                        .map(|v| v.value.clone().to_lowercase());

                                    let opaque =
                                        vec![Some("count".to_string()), Some("sum".to_string())]
                                            .contains(&function_name);

                                    for arg in &function.args {
                                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) =
                                            arg
                                        {
                                            let cols = Self::get_column_names_from_expr(expr);
                                            for col in &cols {
                                                // This col could be compound and have a table
                                                // name in it. This is a bad way to do it and
                                                // should be fixed.

                                                let col = col.split('.').collect::<Vec<&str>>();
                                                let column_name =
                                                    col.last().expect("Failed to get any columns");

                                                if col.len() > 1 {
                                                    out_col_table = Some(col.first().expect("Failed to get table from column string").to_string());
                                                }

                                                self.search_for_col_and_add(
                                                    select.from.clone(),
                                                    column_name.to_string(),
                                                    dependency_alias.clone(),
                                                    out_col_table.clone(),
                                                    opaque,
                                                );
                                            }
                                        }
                                    }
                                }
                                Expr::Identifier(id) => {
                                    output_col = id.value.clone();
                                }
                                _ => unimplemented!("{expr:?} is not implemented"),
                            }
                        }
                        SelectItem::QualifiedWildcard(_, _) => {
                            panic!("Wild card not supported")
                        }
                        SelectItem::Wildcard(_) => panic!("Wild card not supported"),
                    }

                    if !output_col.is_empty() {
                        self.search_for_col_and_add(
                            select.from.clone(),
                            output_col.clone(),
                            dependency_alias.clone(),
                            out_col_table.clone(),
                            false,
                        );
                    }
                }
            }
            // This is for unions
            SetExpr::SetOperation { left, right, .. } => {
                self.traverse_set_expr(*left);
                self.traverse_set_expr(*right);
            }
            _ => unimplemented!(),
        }
    }

    pub fn traverse_statements(&mut self, statements: Vec<Statement>) {
        for statement in &statements {
            let body = if let Statement::Query(query) = statement {
                *query.body.clone()
            } else {
                continue;
            };
            self.traverse_set_expr(body);
        }
    }
    pub fn get_query_lineage(&mut self, sql: String) -> Result<(), Box<dyn std::error::Error>> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};

        let statements = Parser::parse_sql(&dialect, &sql)?;
        self.traverse_statements(statements);

        Ok(())
    }
    fn get_column_names_from_expr(expr: &Expr) -> Vec<String> {
        let mut out = Vec::new();
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                let left = Self::get_column_names_from_expr(left);
                let right = Self::get_column_names_from_expr(right);
                out.extend(left);
                out.extend(right);
            }
            Expr::Identifier(id) => {
                out.push(id.value.clone());
            }
            Expr::CompoundIdentifier(ids) => {
                let ids = ids.iter().map(|v| v.value.clone()).collect::<Vec<String>>();
                let id = ids.join(".");

                out.push(id);
            }
            _ => unimplemented!(),
        }
        out
    }
}
impl From<Vec<FullColumn>> for QueryAnalytics {
    fn from(metadata: Vec<FullColumn>) -> Self {
        Self {
            metadata,
            dependency_map: HashMap::new(),
        }
    }
}
impl From<HashMap<String, (Vec<String>, bool)>> for QueryAnalytics {
    fn from(dependency_map: HashMap<String, (Vec<String>, bool)>) -> Self {
        Self {
            dependency_map,
            metadata: Vec::new(),
        }
    }
}
