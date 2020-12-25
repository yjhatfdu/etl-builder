"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class Context {
    constructor() {
        this.counter = 0;
        this.nodes = [];
        this.taskInfo = {};
        this.taskName = "";
    }
    name(name) {
        this.taskName = name;
        return this;
    }
    column(...names) {
        return new ColumnNode(this, names.map(n => n.split(".").length == 3 ? n : this.defaultTable() + '.' + n));
    }
    const(...expressions) {
        return new ConstNode(this, expressions);
    }
    concat(...nodes) {
        return new ConcatNode(this, nodes);
    }
    dataSource(ds) {
        this.taskInfo["DataSourceName"] = ds;
        return this;
    }
    //todo join option
    sourceTable(schema, table) {
        if (this.taskInfo.SourceTable == null) {
            this.taskInfo.SourceTable = [];
        }
        this.taskInfo.SourceTable.push({
            "Schema": schema,
            "Table": table
        });
        return this;
    }
    primaryKeys(...keys) {
        this.taskInfo.PrimaryKeys = keys;
        return this;
    }
    fetchCount(count) {
        this.taskInfo.FetchCount = count;
        return this;
    }
    parallel(count) {
        this.taskInfo.Workers = count;
        return this;
    }
    outPrimaryKeys(...pks) {
        this.taskInfo.OutPrimaryKeys = pks;
        return this;
    }
    concatTargetColumns(targetColumn, ...sourceColumns) {
        if (!this.taskInfo.TargetColumnsForConcat) {
            this.taskInfo.TargetColumnsForConcat = [];
        }
        this.taskInfo.TargetColumnsForConcat.push({
            TargetColumn: targetColumn,
            SourceColumns: sourceColumns
        });
    }
    dbSink(dataSource, schema, table, upsert = false, autoTruncate = false) {
        if (this.taskInfo.Sinks == null) {
            this.taskInfo.Sinks = [];
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
        return this;
    }
    empiSink(empiHost, worker, identifier) {
        if (this.taskInfo.Sinks == null) {
            this.taskInfo.Sinks = [];
        }
        const args = {
            "Host": empiHost,
            "Worker": worker,
        };
        Object.keys(identifier).forEach(c => {
            args[c] = identifier[c];
        });
        this.taskInfo.Sinks.push({
            Args: args,
            Type: "empi"
        });
        return this;
    }
    useOutPutAggregation(orderByColumn = "", desc = false, aggrType = "last") {
        this.taskInfo.OutputAggregation = {
            UseAggr: true,
            AggrType: "last",
            OrderByColumn: orderByColumn,
            Desc: desc
        };
        return this;
    }
    build(sourceTransform) {
        let nodes = this.nodes.map(n => n.build(sourceTransform));
        return { Graph: nodes.filter(x => !!x), Info: this.taskInfo, Name: this.taskName };
    }
    defaultTable() {
        return this.taskInfo.SourceTable[0].Schema + '.' + this.taskInfo.SourceTable[0].Table;
    }
}
exports.Context = Context;
class Node {
    constructor(ctx, type, lastNode) {
        try {
            throw new Error();
        }
        catch (e) {
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
            this.fields = lastNode.fields;
        }
    }
    pipe(f) {
        return f(this);
    }
    rename(...name) {
        name.forEach((n, i) => n === '-' ? true : this.fields[i] = n);
        return this;
    }
    field(...fieldName) {
        return new FieldNode(this.ctx, this, fieldName);
    }
    select(...fieldNum) {
        return new SelectNode(this.ctx, this, fieldNum);
    }
    output(name) {
        new TargetNode(this.ctx, this, name);
    }
    filter(expr) {
        return new FilterNode(this.ctx, this, expr);
    }
    aggr(type, argsExprs, groupByExpr, orderByExpr, desc) {
        return new AggrNode(this.ctx, this, type, argsExprs, groupByExpr, orderByExpr, desc);
    }
    map(...expr) {
        return new MapNode(this.ctx, this, expr, null);
    }
    mapQuery(queryArgs) {
        return new MapNode(this.ctx, this, null, queryArgs);
    }
    summary(type, expr, ...args) {
        return new SummaryNode(this.ctx, this, type, expr, ...args);
    }
    build(sourceTransform) {
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
        };
    }
}
class ColumnNode extends Node {
    constructor(ctx, names) {
        super(ctx, 'column', null);
        this.names = names.slice();
        this.fields = names.slice();
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify(this.names);
        return n;
    }
}
class ConstNode extends Node {
    constructor(ctx, expressions) {
        super(ctx, 'const', null);
        this.expressions = expressions;
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        if (this.expressions) {
            n.HandlerType = 'expr';
            n.HandlerArgs = JSON.stringify(this.expressions);
        }
        else if (this.query) {
            n.HandlerType = 'query';
            n.HandlerArgs = JSON.stringify(this.query);
        }
        return n;
    }
}
class TargetNode extends Node {
    constructor(ctx, lastNode, name) {
        super(ctx, "output", lastNode);
        this.name = name;
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = this.name;
        return n;
    }
}
class MapNode extends Node {
    constructor(ctx, lastNode, expressions, query) {
        super(ctx, 'map', lastNode);
        this.expressions = expressions;
        this.query = query;
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        if (this.expressions) {
            n.HandlerType = 'expr';
            n.HandlerArgs = JSON.stringify(this.expressions);
        }
        else if (this.query) {
            n.HandlerType = 'query';
            n.HandlerArgs = JSON.stringify(this.query);
        }
        return n;
    }
}
function mapExpr(...expr) {
    return function (node) {
        return new MapNode(node.ctx, node, expr, null);
    };
}
function mapQuery(queryArgs) {
    return function (node) {
        return new MapNode(node.ctx, node, null, queryArgs);
    };
}
class FieldNode extends Node {
    constructor(ctx, lastNode, fields) {
        super(ctx, 'field', lastNode);
        this.fieldIndex = [];
        this.fields = fields;
        this.fieldIndex = fields.map(f => lastNode.fields.indexOf(f));
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify(this.fieldIndex);
        return n;
    }
}
class SelectNode extends Node {
    constructor(ctx, lastNode, fields) {
        super(ctx, 'field', lastNode);
        this.fieldIndex = [];
        this.fieldIndex = fields.map(f => f - 1);
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify(this.fieldIndex);
        return n;
    }
}
class ConcatNode extends Node {
    constructor(ctx, parentNodes) {
        super(ctx, 'concat', null);
        this.parentNodes = parentNodes;
        parentNodes.forEach(p => {
            p.next.push(this);
            this.prev.push(p);
        });
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeType = 'map';
        n.HandlerType = 'empty';
        return n;
    }
}
class FilterNode extends Node {
    constructor(ctx, lastNode, expr) {
        super(ctx, "filter", lastNode);
        this.expr = expr;
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = this.expr;
        return n;
    }
}
exports.SummaryType = {
    count: "count", sum: "sum", notNullPercent: "notNullPercent", groupCount: "groupCount", histogram: "histogram"
};
class SummaryNode extends Node {
    constructor(ctx, lastNode, type, expr, ...args) {
        super(ctx, "summary", lastNode);
        this.expr = expr;
        this.stype = type;
        this.args = args;
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify({ expr: this.expr, type: this.stype, args: this.args });
        return n;
    }
}
class AggrNode extends Node {
    constructor(ctx, lastNode, type, argsExpr, groupByExpr, orderByExpr, desc) {
        super(ctx, "aggr", lastNode);
        this.aggType = type;
        this.argsExpr = argsExpr;
        this.groupByExpr = groupByExpr;
        this.orderByExpr = orderByExpr;
        this.desc = desc;
    }
    build(sourceTransform) {
        let n = super.build(sourceTransform);
        n.NodeArgs = JSON.stringify({
            AggType: this.aggType,
            AggArgsExpr: this.argsExpr,
            GroupByExpr: this.groupByExpr,
            OrderByExpr: this.orderByExpr,
            Desc: this.desc
        });
        return n;
    }
}
exports.defaultContext = new Context();
exports.name = (n) => exports.defaultContext.name(n);
exports.column = (...col) => exports.defaultContext.column(...col);
exports.Const = (...expr) => exports.defaultContext.const(...expr);
exports.concat = (...node) => exports.defaultContext.concat(...node);
exports.dataSource = (ds) => exports.defaultContext.dataSource(ds);
exports.sourceTable = (schema, table) => exports.defaultContext.sourceTable(schema, table);
exports.primaryKeys = (...keys) => exports.defaultContext.primaryKeys(...keys);
exports.fetchCount = (count) => exports.defaultContext.fetchCount(count);
exports.parallel = (count) => exports.defaultContext.parallel(count);
exports.outPrimaryKeys = (...pks) => exports.defaultContext.outPrimaryKeys(...pks);
exports.dbSink = (dataSource, schema, table, upsert = false, autoTruncate = false) => exports.defaultContext.dbSink(dataSource, schema, table, upsert, autoTruncate);
exports.useOutPutAggregation = (orderByColumn = "", desc = false, aggrType = "last") => exports.useOutPutAggregation(orderByColumn, desc, aggrType);
//# sourceMappingURL=index.js.map