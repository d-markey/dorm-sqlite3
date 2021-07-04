import 'dart:async';

import 'package:dorm/dorm.dart';
import 'package:sqlite3/sqlite3.dart';

import 'extensions.dart';
import 'SQLiteTransaction.dart';

class SQLiteDatabase extends DormDatabase {
  SQLiteDatabase(this._database);

  final Database _database;

  @override
  final defaultKeyName = 'rowid';

  int? _dbToInt(dynamic value) => value as int?;
  bool? _dbToBool(dynamic value) => (value == null) ? null : (_dbToInt(value) == 0 ? false : true);
  num? _dbToNum(dynamic value) => value as num?;
  String? _dbToString(dynamic value) => value as String?;
  DateTime? _dbToDateTime(dynamic value) => (value == null) ? null : DateTime.fromMillisecondsSinceEpoch(1000 * _dbToInt(value)!, isUtc: true);
  List<int>? _dbToBlob(dynamic value) => (value as Iterable).map((e) => e as int).asList();

  @override
  T? castFromDb<T>(dynamic value) {
    if (value == null) return null;
    if (isType<T, int>()) return _dbToInt(value) as T?;
    if (isType<T, bool>()) return _dbToBool(value) as T?;
    if (isType<T, num>()) return  _dbToNum(value) as T?;
    if (isType<T, DateTime>()) return _dbToDateTime(value) as T?;
    if (isType<T, String>()) return _dbToString(value) as T?;
    if (isList<T, int>()) return _dbToBlob(value) as T?;
    throw DormException('unsupported type $T (value = $value)');
  }

  int? intToDb(int? value) => value;
  int? boolToDb(bool? value) => (value == null) ? null : (value ? 1 : 0);
  num? numToDb(num? value) => value;
  String? stringToDb(String? value) => value;
  int? dateTimeToDb(DateTime? value) => (value == null) ? null : (value.toUtc().millisecondsSinceEpoch ~/ 1000);
  List<int> blobToDb(Iterable<int>? value) => value!.asList();

  @override
  dynamic castToDb(dynamic value) {
    if (value == null) return null;
    if (value is int) return intToDb(value);
    if (value is bool) return boolToDb(value);
    if (value is num) return  numToDb(value);
    if (value is DateTime) return dateTimeToDb(value);
    if (value is String) return stringToDb(value);
    if (value is Iterable<int>) return blobToDb(value);
    throw DormException('unexpected type ${value.runtimeType} for $value');
  }


  int execute(String sql, [ List<Object?> parameters = const [] ]) {
    try {
      _database.execute(sql, parameters);
      return _database.lastInsertRowId;
    } on SqliteException catch (ex) {
      throw DormException('SQLite3 execution failed', inner: ex);
    }
  }

  Future<int> count(String sql, [ List<Object?> parameters = const [] ]) {
    ResultSet rs;
    try {
      rs = _database.select(sql, parameters);
    } on SqliteException catch (ex) {
      throw DormException('SQLite3 select failed', inner: ex, data: { 'sql': sql, 'parameters': parameters });
    }
    if (rs.rows.isEmpty) {
      return Future.value(0);
    } else {
      return Future.value(rs.rows.first[0] as int);
    }
  }

  Future<Iterable<DormRecord>> select(String sql, [ List<Object?> parameters = const [] ]) {
    ResultSet rs;
    try {
      rs = _database.select(sql, parameters);
    } on SqliteException catch (ex) {
      throw DormException('SQLite3 select failed', inner: ex);
    }
    final items = <DormRecord>[];
    final columnNames = rs.columnNames;
    for (var i = 0; i < rs.rows.length; i++) {
      // ignore: prefer_collection_literals
      var item = DormRecord();
      for (var j = 0; j < columnNames.length; j++) {
        item[columnNames[j]] = rs.rows[i][j];
      }
      items.add(item);
    }
    return Future.value(items);
  }

  @override
  void dispose() {
    if (_transactionLevel != 0) {
      throw DormException('A transaction is pending');
    }
    _database.dispose();
  }

  int _transactionLevel = 0;

  @override
  Future<T> transaction<T>(DormWorker<T> work) async {
    if (_transactionLevel++ == 0) {
      execute('BEGIN TRANSACTION;');
    }
    var success = false;
    final transaction = SQLiteTransaction(this);
    try {
      final result = await work(transaction);
      success = true;
      return result;
    } finally {
      transaction.dispose();
      if (--_transactionLevel < 0) {
      } else if (_transactionLevel == 0) {
        execute(success ? 'COMMIT;' : 'ROLLBACK;');
      }
    }
  }

  @override
  Future<T> readonly<T>(DormWorker<T> work) async {
    final transaction = SQLiteTransaction(this);
    try {
      final result = await work(transaction);
      return result;
    } finally {
      transaction.dispose();
    }
  }
}
