export interface SourceTable {
    Schema: string
    Table: string
}

export interface Sink {
    Type: string
    Args: any
}

export interface TaskInfo {
    SourceTable?: SourceTable[]
    PrimaryKeys?: string[]
    FetchCount?: number
    Workers?: number
    OutPrimaryKeys?: string[]
    Sinks?: Sink[]
    OutputAggregation?: OutputAggr
}

export interface OutputAggr {
    UseAggr: boolean,
    AggrType: "last",//目前只支持last
    OrderByColumn: string,
    Desc: boolean

}

export interface QueryClause {
    column: string
    operator: "=" | ">" | "<" | "like" | ">=" | "<="
    value: string
}

export interface QueryArgs {
    cacheSize: number,
    dataSource: string,
    targetTable: string,
    targetColumns: string[],
    query: QueryClause[]
}

export class Context {
    counter = 0;
    nodes: Node[] = [];
    taskInfo: TaskInfo = {};
    taskName = "";

    name(name: string) {
        this.taskName = name;
        return this
    }

    column(...names: string[]):Node {
        return new ColumnNode(this, names.map(n => n.split(".").length == 3 ? n : this.defaultTable() + '.' + n))
    }

    const(...expressions: string[]):Node {
        return new ConstNode(this, expressions)
    }

    concat(...nodes: Node[]):Node {
        return new ConcatNode(this, nodes)
    }

    dataSource(ds: string) {
        this.taskInfo["DataSourceName"] = ds;
        return this
    }

    //todo join option
    sourceTable(schema: string, table: string,) {
        if (this.taskInfo.SourceTable == null) {
            this.taskInfo.SourceTable = []
        }
        this.taskInfo.SourceTable.push({
            "Schema": schema,
            "Table": table
        });
        return this
    }

    primaryKeys(...keys: string[]) {
        this.taskInfo.PrimaryKeys = keys;
        return this
    }

    fetchCount(count: number) {
        this.taskInfo.FetchCount = count;
        return this
    }

    parallel(count: number) {
        this.taskInfo.Workers = count;
        return this
    }

    outPrimaryKeys(...pks: string[]) {
        this.taskInfo.OutPrimaryKeys = pks;
        return this
    }

    dbSink(dataSource: string, schema: string, table: string, upsert = false, autoTruncate = false) {
        if (this.taskInfo.Sinks == null) {
            this.taskInfo.Sinks = []
        }
        this.taskInfo.Sinks.push({
            Args: {
                "DataSource": dataSource,
                "Schema": schema,
                "Table": table,
                "Upsert": upsert,
                "TruncateBeforeStart": autoTruncate
            },
            Type: "db"
        });
        return this
    }

    empiSink(empiHost: string, worker: number, identifier: any) {
        if (this.taskInfo.Sinks == null) {
            this.taskInfo.Sinks = []
        }

        const args = {
            "Host": empiHost,
            "Worker": worker,
        };
        Object.keys(identifier).forEach(c => {
            args[c] = identifier[c]
        });
        this.taskInfo.Sinks.push({
            Args: args,
            Type: "empi"
        });
        return this
    }

    useOutPutAggregation(orderByColumn = "", desc = false, aggrType = "last") {
        this.taskInfo.OutputAggregation = {
            UseAggr: true,
            AggrType: "last",//目前只支持last
            OrderByColumn: orderByColumn,
            Desc: desc
        };
        return this
    }

    build(sourceTransform?) {
        let nodes = this.nodes.map(n => n.build(sourceTransform));
        return {Graph: nodes.filter(x => !!x), Info: this.taskInfo, Name: this.taskName}
    }

    defaultTable() {
        return this.taskInfo.SourceTable[0].Schema + '.' + this.taskInfo.SourceTable[0].Table
    }
}

class Node {
    prev: Node[];
    next: Node[];
    ctx: Context;
    type: string;
    index: number;
    line: number;
    pos: number;
    fields;
    context: string;

    constructor(ctx: Context, type: string, lastNode: Node) {
        try {
            throw new Error();
        } catch (e) {
            this.line = +e.stack.match(/<anonymous>:(\d+):(\d+)/)[1];
            this.pos = +e.stack.match(/<anonymous>:(\d+):(\d+)/)[2];
        }
        this.prev = [];
        this.next = [];
        this.ctx = ctx;
        this.type = type;
        this.index = ctx.counter++;
        ctx.nodes[this.index] = this;
        if (lastNode) {
            this.prev.push(lastNode);
            lastNode.next.push(this);
            this.fields = lastNode.fields
        }
    }

    pipe(f: (Node) => Node) {
        return f(this)
    }

    rename(...name: string[]) {
        name.forEach((n, i) => n === '-' ? true : this.fields[i] = n);
        return this;
    }

    field(...fieldName: string[]) {
        return new FieldNode(this.ctx, this, fieldName)
    }

    select(...fieldNum: number[]) {
        return new SelectNode(this.ctx, this, fieldNum)
    }

    output(name: string) {
        new TargetNode(this.ctx, this, name)
    }

    filter(expr: string) {
        return new FilterNode(this.ctx, this, expr)
    }

       aggr(type: string, argsExprs: string[], groupByExpr: string, orderByExpr: string, desc: boolean) {
        return new AggrNode(this.ctx, this, type, argsExprs, groupByExpr, orderByExpr, desc)
    }

    map(...expr: string[]) {
        return new MapNode(this.ctx, this, expr, null);
    }

    mapQuery(queryArgs: QueryArgs) {
        return new MapNode(this.ctx, this, null, queryArgs)
    }

    summary(type: string, expr: string[], ...args: any) {
        return new SummaryNode(this.ctx, this, type, expr, ...args)
    }

    build(sourceTransform: (line: number, pos: number) => [number, number, string]) {
        let line = this.line;
        let pos = this.pos;
        let codectx;
        if (sourceTransform) {
            [line, pos, codectx] = sourceTransform(line, pos);
        }
        return {
            Index: this.index,
            NodeType: this.type,
            Pre: this.prev.map(n => n.index),
            Next: this.next.map(n => n.index),
            Context: codectx,
            Line: line,
            Column: pos,
            Id: "" + this.index,
            NodeArgs: null,
            HandlerType: null,
            HandlerArgs: null
        }
    }
}

class ColumnNode extends Node {
    names;

    constructor(ctx: Context, names: string[]) {
        super(ctx, 'column', null);
        this.names = names.slice();
        this.fields = names.slice();
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify(this.names);
        return n
    }
}

class ConstNode extends Node {
    expressions;
    query: QueryArgs;

    constructor(ctx: Context, expressions: string[]) {
        super(ctx, 'const', null);
        this.expressions = expressions;
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        if (this.expressions) {
            n.HandlerType = 'expr';
            n.HandlerArgs = JSON.stringify(this.expressions)
        } else if (this.query) {
            n.HandlerType = 'query';
            n.HandlerArgs = JSON.stringify(this.query)
        }
        return n;
    }
}

class TargetNode extends Node {
    name;

    constructor(ctx: Context, lastNode: Node, name: string) {
        super(ctx, "output", lastNode);
        this.name = name;
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = this.name;
        return n
    }
}

class MapNode extends Node {
    expressions: string[];
    query: QueryArgs;

    constructor(ctx: Context, lastNode: Node, expressions: string[], query: QueryArgs) {
        super(ctx, 'map', lastNode);
        this.expressions = expressions;
        this.query = query;
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        if (this.expressions) {
            n.HandlerType = 'expr';
            n.HandlerArgs = JSON.stringify(this.expressions)
        } else if (this.query) {
            n.HandlerType = 'query';
            n.HandlerArgs = JSON.stringify(this.query)
        }
        return n;
    }
}

function mapExpr(...expr: string[]) {
    return function (node) {
        return new MapNode(node.ctx, node, expr, null);
    }
}

function mapQuery(queryArgs: QueryArgs) {
    return function (node) {
        return new MapNode(node.ctx, node, null, queryArgs);
    }
}

class FieldNode extends Node {
    fieldIndex = [];

    constructor(ctx: Context, lastNode: Node, fields: string[]) {
        super(ctx, 'field', lastNode);
        this.fields = fields;
        this.fieldIndex = fields.map(f => lastNode.fields.indexOf(f))
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify(this.fieldIndex);
        return n
    }
}

class SelectNode extends Node {
    fieldIndex = [];

    constructor(ctx: Context, lastNode: Node, fields: number[]) {
        super(ctx, 'field', lastNode);
        this.fieldIndex = fields.map(f => f - 1)
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify(this.fieldIndex);
        return n
    }
}

class ConcatNode extends Node {
    parentNodes;

    constructor(ctx: Context, parentNodes: Node[]) {
        super(ctx, 'concat', null);
        this.parentNodes = parentNodes;
        parentNodes.forEach(p => {
            p.next.push(this);
            this.prev.push(p);
        })
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeType = 'map';
        n.HandlerType = 'empty';
        return n
    }
}

class FilterNode extends Node {
    expr;

    constructor(ctx: Context, lastNode: Node, expr: string) {
        super(ctx, "filter", lastNode);
        this.expr = expr;
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = this.expr;
        return n
    }
}

export const SummaryType = {
    count: "count", sum: "sum", notNullPercent: "notNullPercent", groupCount: "groupCount", histogram: "histogram"
};

class SummaryNode extends Node {
    expr;
    stype;
    args;

    constructor(ctx: Context, lastNode: Node, type: string, expr: string[], ...args: any[]) {
        super(ctx, "summary", lastNode);
        this.expr = expr;
        this.stype = type;
        this.args = args
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify({expr: this.expr, type: this.stype, args: this.args});
        return n
    }
}

class AggrNode extends Node {
    aggType;
    argsExpr;
    groupByExpr;
    orderByExpr;
    desc;

    constructor(ctx: Context, lastNode: Node, type: string, argsExpr: string[], groupByExpr: string, orderByExpr: string, desc: boolean) {
        super(ctx, "aggr", lastNode);
        this.aggType = type;
        this.argsExpr = argsExpr;
        this.groupByExpr = groupByExpr;
        this.orderByExpr = orderByExpr;
        this.desc = desc
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify({
            AggType: this.aggType,
            AggArgsExpr: this.argsExpr,
            GroupByExpr: this.groupByExpr,
            OrderByExpr: this.orderByExpr,
            Desc: this.desc
        });
        return n
    }
}


export const defaultContext = new Context();
export const name = (n: string) => defaultContext.name(n);
export const column = (...col: string[]):Node => defaultContext.column(...col);
export const Const = (...expr: string[]):Node  => defaultContext.const(...expr);
export const concat = (...node: Node[]):Node  => defaultContext.concat(...node);
export const dataSource = (ds: string) => defaultContext.dataSource(ds);
export const sourceTable = (schema: string, table: string) => defaultContext.sourceTable(schema, table);
export const primaryKeys = (...keys: string[]) => defaultContext.primaryKeys(...keys);
export const fetchCount = (count: number) => defaultContext.fetchCount(count);
export const parallel = (count: number) => defaultContext.parallel(count);
export const outPrimaryKeys = (...pks: string[]) => defaultContext.outPrimaryKeys(...pks);
export const dbSink = (dataSource: string, schema: string, table: string, upsert = false, autoTruncate = false) =>
    defaultContext.dbSink(dataSource, schema, table, upsert, autoTruncate);
export const useOutPutAggregation = (orderByColumn = "", desc = false, aggrType = "last") => useOutPutAggregation(orderByColumn, desc, aggrType);

