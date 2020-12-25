export interface SourceTable {
    Schema: string;
    Table: string;
}
export interface Sink {
    Type: string;
    Args: any;
}
export interface TargetColumnForConcat {
    TargetColumn: string;
    SourceColumns: string[];
}
export interface TaskInfo {
    SourceTable?: SourceTable[];
    PrimaryKeys?: string[];
    FetchCount?: number;
    Workers?: number;
    OutPrimaryKeys?: string[];
    Sinks?: Sink[];
    OutputAggregation?: OutputAggr;
    TargetColumnsForConcat?: TargetColumnForConcat[];
}
export interface OutputAggr {
    UseAggr: boolean;
    AggrType: "last";
    OrderByColumn: string;
    Desc: boolean;
}
export interface QueryClause {
    column: string;
    operator: "=" | ">" | "<" | "like" | ">=" | "<=";
    value: string;
}
export interface QueryAggr {
    column: string;
}
export interface QueryOrder {
    column: string;
    desc: boolean;
}
export interface QueryArgs {
    cacheSize: number;
    dataSource: string;
    targetTable: string;
    targetColumns: string[];
    query: QueryClause[];
    aggr: QueryAggr[];
    order: QueryOrder[];
}
export declare class Context {
    counter: number;
    nodes: Node[];
    taskInfo: TaskInfo;
    taskName: string;
    name(name: string): this;
    column(...names: string[]): Node;
    const(...expressions: string[]): Node;
    concat(...nodes: Node[]): Node;
    dataSource(ds: string): this;
    sourceTable(schema: string, table: string): this;
    primaryKeys(...keys: string[]): this;
    fetchCount(count: number): this;
    parallel(count: number): this;
    outPrimaryKeys(...pks: string[]): this;
    concatTargetColumns(targetColumn: string, ...sourceColumns: string[]): void;
    dbSink(dataSource: string, schema: string, table: string, upsert?: boolean, autoTruncate?: boolean): this;
    empiSink(empiHost: string, worker: number, identifier: any): this;
    useOutPutAggregation(orderByColumn?: string, desc?: boolean, aggrType?: string): this;
    build(sourceTransform?: any): {
        Graph: {
            Index: number;
            NodeType: string;
            Pre: number[];
            Next: number[];
            Context: any;
            Line: number;
            Column: number;
            Id: string;
            NodeArgs: any;
            HandlerType: any;
            HandlerArgs: any;
        }[];
        Info: TaskInfo;
        Name: string;
    };
    defaultTable(): string;
}
declare class Node {
    prev: Node[];
    next: Node[];
    ctx: Context;
    type: string;
    index: number;
    line: number;
    pos: number;
    fields: any;
    context: string;
    constructor(ctx: Context, type: string, lastNode: Node);
    pipe(f: (Node: any) => Node): Node;
    rename(...name: string[]): this;
    field(...fieldName: string[]): FieldNode;
    select(...fieldNum: number[]): SelectNode;
    output(name: string): void;
    filter(expr: string): FilterNode;
    aggr(type: string, argsExprs: string[], groupByExpr: string, orderByExpr: string, desc: boolean): AggrNode;
    map(...expr: string[]): MapNode;
    mapQuery(queryArgs: QueryArgs): MapNode;
    summary(type: string, expr: string[], ...args: any): SummaryNode;
    build(sourceTransform: (line: number, pos: number) => [number, number, string]): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
declare class MapNode extends Node {
    expressions: string[];
    query: QueryArgs;
    constructor(ctx: Context, lastNode: Node, expressions: string[], query: QueryArgs);
    build(sourceTransform?: any): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
declare class FieldNode extends Node {
    fieldIndex: any[];
    constructor(ctx: Context, lastNode: Node, fields: string[]);
    build(sourceTransform?: any): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
declare class SelectNode extends Node {
    fieldIndex: any[];
    constructor(ctx: Context, lastNode: Node, fields: number[]);
    build(sourceTransform?: any): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
declare class FilterNode extends Node {
    expr: any;
    constructor(ctx: Context, lastNode: Node, expr: string);
    build(sourceTransform?: any): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
export declare const SummaryType: {
    count: string;
    sum: string;
    notNullPercent: string;
    groupCount: string;
    histogram: string;
};
declare class SummaryNode extends Node {
    expr: any;
    stype: any;
    args: any;
    constructor(ctx: Context, lastNode: Node, type: string, expr: string[], ...args: any[]);
    build(sourceTransform?: any): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
declare class AggrNode extends Node {
    aggType: any;
    argsExpr: any;
    groupByExpr: any;
    orderByExpr: any;
    desc: any;
    constructor(ctx: Context, lastNode: Node, type: string, argsExpr: string[], groupByExpr: string, orderByExpr: string, desc: boolean);
    build(sourceTransform?: any): {
        Index: number;
        NodeType: string;
        Pre: number[];
        Next: number[];
        Context: any;
        Line: number;
        Column: number;
        Id: string;
        NodeArgs: any;
        HandlerType: any;
        HandlerArgs: any;
    };
}
export declare const defaultContext: Context;
export declare const name: (n: string) => Context;
export declare const column: (...col: string[]) => Node;
export declare const Const: (...expr: string[]) => Node;
export declare const concat: (...node: Node[]) => Node;
export declare const dataSource: (ds: string) => Context;
export declare const sourceTable: (schema: string, table: string) => Context;
export declare const primaryKeys: (...keys: string[]) => Context;
export declare const fetchCount: (count: number) => Context;
export declare const parallel: (count: number) => Context;
export declare const outPrimaryKeys: (...pks: string[]) => Context;
export declare const dbSink: (dataSource: string, schema: string, table: string, upsert?: boolean, autoTruncate?: boolean) => Context;
export declare const useOutPutAggregation: (orderByColumn?: string, desc?: boolean, aggrType?: string) => any;
export {};
