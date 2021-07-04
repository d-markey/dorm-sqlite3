library dorm_sqlite3;

import 'dart:io';

import 'package:dorm/dorm_interface.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:path_provider/path_provider.dart';

import 'src/SQLiteDatabase.dart';

class DormSqlite3DatabaseProvider extends IDormDatabaseProvider {
  Future<IDormDatabase> _configure(Database sqliteDb, IDormConfiguration configuration) async {
    final db = SQLiteDatabase(sqliteDb);
    await configuration.applyTo(db);
    return db;
  }

  @override
  Future<IDormDatabase> openDatabase(String databaseName, IDormConfiguration configuration, { bool inMemory = false, bool reset = false }) async {
    Database sqliteDb;
    if (inMemory) {
      sqliteDb = sqlite3.openInMemory();
    } else {
      final appSupportDir = await getApplicationDocumentsDirectory();
      final file = File('${appSupportDir.path}${Platform.pathSeparator}$databaseName.db');
      if (reset && await file.exists()) {
        await file.delete();
      }
      sqliteDb = sqlite3.open(file.path);
    }
    return await _configure(sqliteDb, configuration);
  }
}
