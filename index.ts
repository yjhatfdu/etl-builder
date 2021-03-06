export interface SourceTable {
    Schema: string
    Table: string
}

export interface Sink {
    Type: string
    Args: any
}

export interface columnToRow {
    TargetColumn?: string
    SourceColumns?: string[]
}

export interface rowToColumnOne {
    MathValue?: string
    SourceColumn?: string
    TargetColumn?: string
}

export interface rowToColumn {
    BaseColumn?: string
    MathColumn?: string
    Transform?: rowToColumnOne[]
}

export interface TaskInfo {
    SourceTable?: SourceTable
    PrimaryKeys?: string[]
    FetchCount?: number
    Workers?: number
    DeduplicateKeys?: string[]
    Sinks?: Sink[]
    OutputAggregation?: OutputAggr
    Init?: boolean,
    CDC?: boolean
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
    value: string | Number
}

export interface QueryAggr {
    column: string
}

export interface QueryOrder {
    column: string
    desc: boolean
}

export interface QueryArgs {
    cacheSize: number,
    dataSource: string,
    targetTable: string,
    targetColumns: string[],
    query: QueryClause[],
    aggr: QueryAggr[],
    order: QueryOrder[],
}

export class Context {
    counter = 0;
    nodes: Node[] = [];
    taskInfo: TaskInfo = {};
    taskName = "";
    taskGroup = 0;
    Priority = 0;
    GroupID = 0;

    _columnToRow: columnToRow[] = [];
    _rowToColumn: rowToColumn = null;


    name(name: string) {
        this.taskName = name;
        return this
    }

    taskgroup(id: number) {
        this.GroupID = id;
        return this
    }

    priority(id: number) {
        this.Priority = id;
        return this
    }

    column(name: string): Node {
        return new ColumnNode(this, name)
    }

    const(...expressions: string[] | number[]): Node {
        return new ConstNode(this, expressions)
    }

    concat(...nodes: Node[]): Node {
        return new ConcatNode(this, nodes)
    }

    dataSource(ds: string) {
        this.taskInfo["DataSourceName"] = ds;
        return this
    }

    //todo join option
    sourceTable(schema: string, table: string,) {
        this.taskInfo.SourceTable = {
            "Schema": schema,
            "Table": table
        };
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

    deduplicateKeys(...pks: string[]) {
        this.taskInfo.DeduplicateKeys = pks;
        return this
    }

    columnToRow(sourceColumns: string[], targetColumn: string) {
        this._columnToRow.push({
            TargetColumn: targetColumn,
            SourceColumns: sourceColumns
        })

        return this
    }

    rowToColumn(baseColumn: string,mathColumn: string,transform: rowToColumnOne[] ){
        this._rowToColumn = {
            BaseColumn: baseColumn,
            MathColumn: mathColumn,
            Transform: transform,
        }

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
    
    jsonSink(){
        this.taskInfo.Sinks.push({
            Args: null,
            Type: "json"
        })
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
        const ret = {
            Layers: [],
            Info: this.taskInfo, 
            Name: this.taskName, 
            Priority: this.Priority, 
            GroupID: this.GroupID
        };

        ret.Layers.push({
            Type:"Graph",
            Graph: nodes.filter(x => !!x), 
        });

        this._columnToRow.length ? ret.Layers.push({
            Type:"ColumnToRow",
            ColumnToRow: this._columnToRow,
        }) : null;

        this._rowToColumn ? ret.Layers.push({
            Type:"RowToColumn",
            RowToColumn: this._rowToColumn
        }) : null;
        
        return ret
    }

    Init() {
        this.taskInfo.Init = true
        return this
    }

    CDC() {
        this.taskInfo.CDC = true
        return this
    }
}

export class Node {
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

    constructor(ctx: Context, names: string) {
        super(ctx, 'column', null);
        this.names = names.slice();
        this.fields = names.slice();
    }

    build(sourceTransform?) {
        let n = super.build(sourceTransform);
        n.NodeArgs = this.names;
        return n
    }
}

class ConstNode extends Node {
    expressions;
    query: QueryArgs;

    constructor(ctx: Context, expressions: string[] | number[]) {
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
