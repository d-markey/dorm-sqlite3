import 'package:dorm/dorm.dart';

class _PageOption {
  int? limit;
  int? startAt;
}

class Query {
  String _sql = '';
  String get sql => _sql;

  final _params = <dynamic>[];
  List<dynamic> get params => _params;
}

String _buildPageExpression(IDormPageClause expr, List<dynamic> params, _PageOption pageOption) {
  if (expr is IDormLimitClause) {
    pageOption.limit ??= expr.max;
    return _buildWhereClause(expr.clause, params, pageOption);
  }
  if (expr is IDormOffsetClause) {
    pageOption.startAt ??= expr.startAt;
    return _buildWhereClause(expr.clause, params, pageOption);
  }
  throw DormException('Unsupported clause $expr');
}

String _buildZeroaryExpression(IDormZeroaryExpression expr, List<dynamic> params, _PageOption pageOption) {
  switch (expr.op) {
    case DormExpressionOperator.All: return '1=1';
    case DormExpressionOperator.None: return '1=0';
    default: throw DormException('Unsupported clause $expr');
  }
}

String _buildUnaryExpression(IDormUnaryExpression expr, List<dynamic> params, _PageOption pageOption) {
  switch (expr.op) {
    case DormExpressionOperator.Not: return 'NOT (${_buildWhereClause(expr.expression, params, pageOption)})';
    default: throw DormException('Unsupported clause $expr');
  }
}

String _buildBinaryExpression(IDormBinaryExpression expr, List<dynamic> params, _PageOption pageOption) {
  switch (expr.op) {
    case DormExpressionOperator.And: return '(${_buildWhereClause(expr.left, params, pageOption)}) AND (${_buildWhereClause(expr.right, params, pageOption)})';
    case DormExpressionOperator.Or: return '(${_buildWhereClause(expr.left, params, pageOption)}) OR (${_buildWhereClause(expr.right, params, pageOption)})';
    default: throw DormException('Unsupported clause $expr');
  }
}

String _buildOperandExpression(IDormOperandExpression expr, List<dynamic> params) {
  if (expr is IDormColumnExpression) {
    return expr.column.name;
  } else {
    final operand = _buildOperandExpression(expr.operand!, params);
    switch (expr.op) {
      case DormExpressionOperator.ToLower:
        return 'LOWER($operand)';
      case DormExpressionOperator.Trim:
        return 'TRIM($operand)';
      case DormExpressionOperator.Length:
        return 'LENGTH($operand)';
      default:      
        throw DormException('Unsupported clause $expr');
    }
  }
}

String _buildRangeExpression(IDormRangeExpression expr, List<dynamic> params, _PageOption pageOption) {
  final operand = _buildOperandExpression(expr.operand, params);
  switch (expr.op) {
    case DormExpressionOperator.InRange:
      if (expr.min == null && expr.max == null) {
        return '1=1';
      } else if (expr.min == null) {
        return '$operand <= ${expr.max}';
      } else if (expr.max == null) {
        return '$operand >= ${expr.min}';
      } else {
        return '($operand >= ${expr.min} AND $operand <= ${expr.max})';
      }
    case DormExpressionOperator.NotInRange:
      if (expr.min == null && expr.max == null) {
        return '1=0';
      } else if (expr.min == null) {
        return '$operand > ${expr.max}';
      } else if (expr.max == null) {
        return '$operand < ${expr.min}';
      } else {
        return '($operand < ${expr.min} OR $operand > ${expr.max})';
      }
    default: throw DormException('Unsupported clause $expr');
  }
}

String _buildComparisonExpression(IDormComparisonExpression expr, List<dynamic> params, _PageOption pageOption) {
  final operand = _buildOperandExpression(expr.operand, params);
  switch (expr.op) {
    case DormExpressionOperator.IsNull:
      return '$operand IS NULL';
    case DormExpressionOperator.IsNotNull:
      return '$operand IS NOT NULL';
    case DormExpressionOperator.Equals:
      params.add(expr.value);
      return '$operand = ?${params.length}';
    case DormExpressionOperator.IsNotEqual:
      params.add(expr.value);
      return '$operand <> ?${params.length}';
    case DormExpressionOperator.LessThan:
      params.add(expr.value);
      return '$operand < ?${params.length}';
    case DormExpressionOperator.LessOrEqual:
      params.add(expr.value);
      return '$operand <= ?${params.length}';
    case DormExpressionOperator.MoreThan:
      params.add(expr.value);
      return '$operand > ?${params.length}';
    case DormExpressionOperator.MoreOrEqual:
      params.add(expr.value);
      return '$operand >= ?${params.length}';
    case DormExpressionOperator.Contains:
      if (expr.value == null) {
        params.add(null);
      } else if (expr.value.length == 0) {
        params.add('%'); 
      } else {
        params.add('%${expr.value}%');
      }
      return '$operand LIKE ?${params.length}';
    case DormExpressionOperator.StartsWith:
      if (expr.value == null) {
        params.add(null);
      } else {
        params.add('${expr.value}%');
      }
      return '$operand LIKE ?${params.length}';
    case DormExpressionOperator.EndsWith:
      if (expr.value == null) {
        params.add(null);
      } else {
        params.add('%${expr.value}');
      }
      return '$operand LIKE ?${params.length}';
    case DormExpressionOperator.InList:
      final values = expr.value as Iterable?;
      if (values == null || values.isEmpty) {
        return '1=0';
      } else {
        final args = <String>[];
        for (var value in values) {
          params.add(value);
          args.add('?${params.length}');
        }
        return '$operand IN (${args.join(', ')})';
      }
    case DormExpressionOperator.NotInList:
      final values = expr.value as Iterable?;
      if (values == null || values.isEmpty) {
        return '1=1';
      } else {
        final args = <String>[];
        for (var value in values) {
          params.add(value);
          args.add('?${params.length}');
        }
        return '$operand NOT IN (${args.join(', ')})';
      }
    default: throw DormException('Unsupported clause $expr');
  }
}

String _buildWhereClause(IDormClause expr, List<dynamic> params, _PageOption pageOption) {
  if (expr is IDormPageClause) return _buildPageExpression(expr, params, pageOption);
  if (expr is IDormRangeExpression) return _buildRangeExpression(expr, params, pageOption);
  if (expr is IDormComparisonExpression) return _buildComparisonExpression(expr, params, pageOption);
  if (expr is IDormZeroaryExpression) return _buildZeroaryExpression(expr, params, pageOption);
  if (expr is IDormUnaryExpression) return _buildUnaryExpression(expr, params, pageOption);
  if (expr is IDormBinaryExpression) return _buildBinaryExpression(expr, params, pageOption);
  throw DormException('Unsupported clause $expr');
}

String _getColumnDef(IDormField column, [ String? keyName ]) {
  String columnDef;
  final columnName = column.name.toLowerCase();
  switch (column.type.toLowerCase()) {
    case 'string':    columnDef = '$columnName TEXT NOT NULL'; break;
    case 'string?':   columnDef = '$columnName TEXT'; break;
    case 'int':       columnDef = '$columnName INTEGER NOT NULL'; break;
    case 'int?':      columnDef = '$columnName INTEGER'; break;
    case 'num':       columnDef = '$columnName REAL NOT NULL'; break;
    case 'num?':      columnDef = '$columnName REAL'; break;
    case 'bool':      columnDef = '$columnName INTEGER NOT NULL'; break;
    case 'bool?':     columnDef = '$columnName INTEGER'; break;
    case 'datetime':  columnDef = '$columnName INTEGER NOT NULL'; break;
    case 'datetime?': columnDef = '$columnName INTEGER'; break;
    case 'blob':      columnDef = '$columnName BLOB NOT NULL'; break;
    case 'blob?':     columnDef = '$columnName BLOB'; break;
    default:          throw DormException('Unsupported type ${column.type}');
  }
  if (columnName == keyName) {
    columnDef = columnDef.replaceAll(' NOT NULL', '') + ' PRIMARY KEY';
  }
  return columnDef;
}

class QueryGenerator {
  static Query createTable(IDormModel model, { required bool withRowId, required Iterable<IDormField> columns, Iterable<IDormIndex>? indexes }) {
    final tableName = model.entityName.toLowerCase();
    final keyName = model.key.name.toLowerCase();
    if (withRowId) {
      columns = columns.where((c) => c.name.toLowerCase() != keyName);
    } else if (columns.every((c) => c.name.toLowerCase() != keyName)) {
      columns = [ model.key ].followedBy(columns);
    }
    final columnDefs = columns.map((c) => _getColumnDef(c, withRowId ? null : keyName)).join(', ');

    final sql = <String>[];

    sql.add('CREATE TABLE $tableName ($columnDefs)${withRowId ? '' : ' WITHOUT ROWID'};');

    if (indexes != null) {
      sql.addAll(indexes.map((i) => createIndex(model, i).sql));
    }

    final createTable = Query();
    createTable._sql = sql.join('\n');
    return createTable;
  }

  static Query renameTable(String name, String newName) {
    final renameTable = Query();
    renameTable._sql = 'ALTER TABLE $name RENAME TO $newName;';
    return renameTable;
  }

  static Query dropTable(IDormModel model) {
    final dropTable = Query();
    dropTable._sql = 'DROP TABLE ${model.entityName};';
    return dropTable;
  }

  static String getIndexName(IDormModel model, IDormIndex index) {
    final idx = index.unique ? 'uidx' : 'idx';
    return '${model.entityName}_${idx}_${index.columns.map((c) => c.name).join('___')}';
  }

  static Query createIndex(IDormModel model, IDormIndex index) {
    final createIndex = Query();
    final indexName = getIndexName(model, index);
    createIndex._sql = 'CREATE ${index.unique ?  'UNIQUE INDEX' : 'INDEX'} $indexName ON ${model.entityName} (${index.columns.map((c) => c.name).join(', ')});';
    return createIndex;
  }

  static Query dropIndex(IDormModel model, IDormIndex index) {
    final dropIndex = Query();
    final indexName = getIndexName(model, index);
    dropIndex._sql = 'DROP INDEX $indexName;';
    return dropIndex;
  }

  static Query alterTable(IDormModel model, { Iterable<IDormField>? deletedColumns, Iterable<IDormField>? newColumns, Iterable<IDormIndex>? newIndexes }) {
    final tableName = model.entityName.toLowerCase();

    final sql = <String>[];

    if (deletedColumns != null) {
      sql.addAll(deletedColumns.map((c) => 'ALTER TABLE $tableName DROP ${c.name};'));
    }

    if (newColumns != null) {
      sql.addAll(newColumns.map((c) => 'ALTER TABLE $tableName ADD ${_getColumnDef(c)};'));
    }

    if (newIndexes != null) {
      sql.addAll(newIndexes.map((i) => QueryGenerator.createIndex(model, i).sql));
    }

    final alterTable = Query();
    alterTable._sql = sql.join('\n');
    return alterTable;
  }

  static Query count(IDormModel model, Iterable<IDormField>? columns, IDormClause? whereClause) {
    final select = Query();
    final pageOption = _PageOption();
    final where = (whereClause == null) ? '' : _buildWhereClause(whereClause, select._params, pageOption);
    select._sql = 'SELECT COUNT(1) FROM ${model.entityName}';
    if (where.isNotEmpty) select._sql += ' WHERE $where';
    if (pageOption.limit != null) select._sql += ' LIMIT ${pageOption.limit}';
    if (pageOption.startAt != null) select._sql += ' OFFSET ${pageOption.startAt}';
    select._sql += ';';
    return select;
  }

  static Query any(IDormModel model, Iterable<IDormField>? columns, IDormClause? whereClause) {
    final select = Query();
    final pageOption = _PageOption();
    final where = (whereClause == null) ? '' : _buildWhereClause(whereClause, select._params, pageOption);
    select._sql = 'SELECT COUNT(1) FROM ${model.entityName}';
    if (where.isNotEmpty) select._sql += ' WHERE $where';
    select._sql += ' LIMIT 1';
    if (pageOption.startAt != null) select._sql += ' OFFSET ${pageOption.startAt}';
    select._sql += ';';
    return select;
  }

  static Query select(IDormModel model, Iterable<IDormField>? columns, IDormClause? whereClause) {
    var cols = Iterable.castFrom(columns ?? model.columns);
    if (!cols.any((c) => c.name == model.key.name)) {
      cols = Iterable.castFrom([ model.key ]).followedBy(cols);
    }
    final select = Query();
    final pageOption = _PageOption();
    final where = (whereClause == null) ? '' : _buildWhereClause(whereClause, select._params, pageOption);
    select._sql = 'SELECT ${cols.map((c) => c.name).join(', ')} FROM ${model.entityName}';
    if (where.isNotEmpty) select._sql += ' WHERE $where';
    if (pageOption.limit != null) select._sql += ' LIMIT ${pageOption.limit}';
    if (pageOption.startAt != null) select._sql += ' OFFSET ${pageOption.startAt}';
    select._sql += ';';
    return select;
  }

  static Query insert(IDormModel model, DormRecord item) {
    final insert = Query();
    final cols = <String>[];
    final vals = <String>[];
    for (var entry in item.entries) {
      if (entry.key != model.key.name || entry.value != null) {
        insert._params.add(entry.value);
        cols.add(entry.key);
        vals.add('?${insert._params.length}');
      }
    }
    insert._sql = 'INSERT INTO ${model.entityName} (${cols.join(', ')}) VALUES (${vals.join(', ')});';
    return insert;
  }

  static Query update(IDormModel model, DormRecord item) {
    final update = Query();
    final cols = <String>[];
    final key = item[model.key.name];
    update._params.add(key);
    final keyParam = '?${update._params.length}';
    for (var entry in item.entries.where((e) => e.key != model.key.name)) {
      update._params.add(entry.value);
      cols.add('${entry.key}=?${update._params.length}');
    }
    update._sql = 'UPDATE ${model.entityName} SET ${cols.join(', ')} WHERE ${model.key.name}=$keyParam';
    return update;
  }

  static Query upsert(IDormModel model, DormRecord item) {
    final upsert = Query();

    final colNames = item.keys.toList();
    for (var i = 0; i < colNames.length; i++) {
      upsert._params.add(item[colNames[i]]);
    }

    final cols = <String>[];
    final vals = <String>[];
    for (var i = 0; i < colNames.length; i++) {
      var colName = colNames[i];
      if (colName != model.key.name || item[colName] != null) {
        cols.add(colName);
        vals.add('?${i+1}');
      }
    }
    var sql = 'INSERT INTO ${model.entityName} (${cols.join(', ')}) VALUES (${vals.join(', ')}) ';

    cols.clear();

    final keyParam = '?${colNames.indexOf(model.key.name)+1}';
    for (var i = 0; i < colNames.length; i++) {
      var colName = colNames[i];
      if (colName != model.key.name) {
        cols.add('$colName=?${i+1}');
      }
    }

    sql += 'ON CONFLICT (${model.key.name}) DO UPDATE SET ${cols.join(', ')} WHERE ${model.key.name}=$keyParam;';

    upsert._sql = sql;
    return upsert;
  }

  static Query delete(IDormModel model, IDormClause? whereClause) {
    final delete = Query();
    final pageOption = _PageOption();
    final sql = (whereClause == null) ? '' : _buildWhereClause(whereClause, delete._params, pageOption);
    final where = sql.isEmpty ? '' : ' WHERE $sql';
    delete._sql = 'DELETE FROM ${model.entityName}$where;';
    return delete;
  }
}