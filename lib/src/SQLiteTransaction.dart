import 'package:dorm/dorm.dart';

import 'SQLiteDatabase.dart';
import 'SQLiteQueryGenerator.dart';
import 'KeyTracker.dart';

class SQLiteTransaction extends IDormTransaction {
  SQLiteTransaction(this._db);

  final SQLiteDatabase _db;

  @override
  IDormDatabase get db => _db;
  final _tracker = KeyTracker();

  static final _completed = Future.value();

  @override
  Future createTable<K, T extends IDormEntity<K>>(IDormModel<K, T> model, { required Iterable<IDormField<T>> columns, Iterable<IDormIndex<T>>? indexes }) {
    var keyName = model.key.name;
    final withRowId = (keyName == _db.defaultKeyName);
    final createTable = QueryGenerator.createTable(model, withRowId: withRowId, columns: columns);
    _db.execute(createTable.sql);
    return _completed;
  }

  @override
  Future addColumns<K, T extends IDormEntity<K>>(IDormModel<K, T> model, { required Iterable<IDormField<T>> columns, Iterable<IDormIndex<T>>? indexes }) {
    final alterTable = QueryGenerator.alterTable(model, newColumns: columns, newIndexes: indexes);
    _db.execute(alterTable.sql);
    return _completed;
  }

  @override
  Future addIndexes<K, T extends IDormEntity<K>>(IDormModel<K, T> model, { required Iterable<IDormIndex<T>>? indexes }) {
    final alterTable = QueryGenerator.alterTable(model, newIndexes: indexes);
    _db.execute(alterTable.sql);
    return _completed;
  }

  @override
  Future renameTable<K, T extends IDormEntity<K>>(IDormModel<K, T> model, String name, String newName) {
    final renameTable = QueryGenerator.renameTable(name, newName);
    _db.execute(renameTable.sql);
    return _completed;
  }

  @override
  Future deleteTable<K, T extends IDormEntity<K>>(IDormModel<K, T> model) {
    final dropTable = QueryGenerator.dropTable(model);
    _db.execute(dropTable.sql);
    return _completed;
  }

  @override
  Future deleteIndexes<K, T extends IDormEntity<K>>(IDormModel<K, T> model, Iterable<IDormIndex<T>> indexesNames) {
    final dropIndexes = indexesNames.map((i) => QueryGenerator.dropIndex(model, i));
    _db.execute(dropIndexes.map((e) => e.sql).join('\n'));
    return _completed;
  }

  @override
  Future deleteColumns<K, T extends IDormEntity<K>>(IDormModel<K, T> model, Iterable<IDormField<T>> columnNames) {
    final alterTable = QueryGenerator.alterTable(model, deletedColumns: columnNames);
    _db.execute(alterTable.sql);
    return _completed;
  }

  @override
  Future<int> dbCount<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) {
    final model = _db.getModel<K, T>();
    final countQuery = QueryGenerator.count(model, [ model.key ], whereClause);
    return _db.count(countQuery.sql, countQuery.params);
  }

  @override
  Future<bool> dbAny<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) {
    final model = _db.getModel<K, T>();
    final selectQuery = QueryGenerator.any(model, [ model.key ], whereClause);
    return _db.count(selectQuery.sql, selectQuery.params).then((res) => res > 0);
  }

  @override
  Future<Iterable<K>> dbLoadKeys<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) {
    final model = _db.getModel<K, T>();
    final selectQuery = QueryGenerator.select(model, [ model.key ], whereClause);
    return _db.select(selectQuery.sql, selectQuery.params).then((rs) => rs.map((e) => e[model.key.name]));
  }

  @override
  bool isTracked(Type entityType, dynamic key) => _tracker.isTracked(entityType, key);

  @override
  Future<Iterable<DormRecord>> dbLoad<K, T extends IDormEntity<K>>([ IDormClause? whereClause ]) async {
    final model = _db.getModel<K, T>();
    final selectQuery = QueryGenerator.select(model, model.columns, whereClause);
    final items = await _db.select(selectQuery.sql, selectQuery.params);
    for (var item in items) {
      _tracker.track(T, item[model.key.name]);
    }
    return items;
  }

  @override
  Future<K> dbUpsert<K, T extends IDormEntity<K>>(DormRecord item) {
    final model = _db.getModel<K, T>();
    if (K == int) {
      var key = item[model.key.name] as int?;
      if (key == null) {
        // INSERT
        final insertQuery = QueryGenerator.insert(model, item);
        key = _db.execute(insertQuery.sql, insertQuery.params);
      } else {
        // UPDATE
        final updateQuery = QueryGenerator.update(model, item);
        _db.execute(updateQuery.sql, updateQuery.params);
      }
      return Future.value(key as K);
    } else {
      // UPSERT
      var key = item[model.key.name] as K;
      final upsertQuery = QueryGenerator.upsert(model, item);
      _db.execute(upsertQuery.sql, upsertQuery.params);
      return Future.value(key);
    }
  }

  @override
  Future dbDelete<K, T extends IDormEntity<K>>(IDormClause whereClause) {
    final model = _db.getModel<K, T>();
    final deleteQuery = QueryGenerator.delete(model, whereClause);
    _db.execute(deleteQuery.sql, deleteQuery.params);
    return _completed;
  }

  void dispose() {
    _tracker.dispose();
  }
}
