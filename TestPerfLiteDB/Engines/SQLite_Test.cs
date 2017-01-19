using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestPerfLiteDB
{
    public class SQLite_Test : ITest
    {
        private string _filename;
        private SQLiteConnection _db;
        private int _count;
        private int _times;

        public int Count { get { return _count; } }
        public int Times { get { return _times; } }
        public int FileLength { get { return (int)new FileInfo(_filename).Length; } }

        public SQLite_Test(int count, int times, string password, bool journal)
        {
            _count = count;
            _times = times;
            _filename = "sqlite-" + Guid.NewGuid().ToString("n") + ".db";
            var cs = "Data Source=" + _filename;
            if (password != null) cs += "; Password=" + password;
            if (journal == false) cs += "; Journal Mode=Off";
            _db = new SQLiteConnection(cs);
        }

        public void Prepare()
        {
            _db.Open();

         /*   var table = new SQLiteCommand("CREATE TABLE col (id INTEGER NOT NULL PRIMARY KEY, name TEXT, lorem TEXT)", _db);
            table.ExecuteNonQuery();*/

            var table2 = new SQLiteCommand("CREATE TABLE col_bulk (id INTEGER NOT NULL PRIMARY KEY, name INTEGER, lorem BLOB)", _db);
            table2.ExecuteNonQuery();
        }

        public void Insert()
        {
            var cmd = new SQLiteCommand("INSERT INTO col (id, name, lorem) VALUES (@id, @name, @lorem)", _db);

            cmd.Parameters.Add(new SQLiteParameter("id", DbType.Int32));
            cmd.Parameters.Add(new SQLiteParameter("name", DbType.Int64));
            cmd.Parameters.Add(new SQLiteParameter("lorem", DbType.Binary));

            foreach (var doc in Helper.GetDocs(_count))
            {
                cmd.Parameters["id"].Value = doc["_id"].AsInt32;
                cmd.Parameters["name"].Value = doc["name"].AsString;
                cmd.Parameters["lorem"].Value = doc["lorem"].AsString;

                cmd.ExecuteNonQuery();
            }
        }

        public void Bulk()
        {
            Random rnd = new Random();
            Byte[] t = new Byte[8];
            Byte[] d = new Byte[42];

            for (int i = 0; i < _times; i++)
            {
                using (var trans = _db.BeginTransaction())
                {
                    var cmd = new SQLiteCommand("INSERT INTO col_bulk (id, name, lorem) VALUES (@id, @name, @lorem)", _db);

                    cmd.Parameters.Add(new SQLiteParameter("id", DbType.Int32));
                    cmd.Parameters.Add(new SQLiteParameter("name", DbType.Int64));
                    cmd.Parameters.Add(new SQLiteParameter("lorem", DbType.Binary));

                    for(int j =0; j< Count; j++)
                    {
                        rnd.NextBytes(t);
                        rnd.NextBytes(d);
                        var ts = BitConverter.ToInt64(t, 0);

                        cmd.Parameters["id"].Value = j + i * Count;
                        cmd.Parameters["name"].Value = BitConverter.ToInt64(t, 0);
                        cmd.Parameters["lorem"].Value = d;

                        cmd.ExecuteNonQuery();
                    }
                 /*   foreach (var doc in Helper.GetDocs(_count, i))
                    {
                        cmd.Parameters["id"].Value = doc["_id"].AsInt32;
                        cmd.Parameters["name"].Value = doc["name"].AsString;
                        cmd.Parameters["lorem"].Value = doc["lorem"].AsString;

                        cmd.ExecuteNonQuery();
                    }*/

                    trans.Commit();
                }
            }
        }

        public void Update()
        {
            var cmd = new SQLiteCommand("UPDATE col SET name = @name, lorem = @lorem WHERE id = @id", _db);

            cmd.Parameters.Add(new SQLiteParameter("id", DbType.Int32));
            cmd.Parameters.Add(new SQLiteParameter("name", DbType.String));
            cmd.Parameters.Add(new SQLiteParameter("lorem", DbType.String));

            foreach (var doc in Helper.GetDocs(_count))
            {
                cmd.Parameters["id"].Value = doc["_id"].AsInt32;
                cmd.Parameters["name"].Value = doc["name"].AsString;
                cmd.Parameters["lorem"].Value = doc["lorem"].AsString;

                cmd.ExecuteNonQuery();
            }
        }

        public void CreateIndex()
        {
            var cmd = new SQLiteCommand("CREATE INDEX idx1 ON col_bulk (name)", _db);

            cmd.ExecuteNonQuery();
        }

        public void Query()
        {
            //var cmd = new SQLiteCommand("SELECT * FROM col_bulk WHERE id = @id ORDER BY name DESC", _db);
            var cmd = new SQLiteCommand("SELECT * FROM col_bulk ORDER BY name ASC", _db);
           // cmd.Parameters.Add(new SQLiteParameter("id", DbType.Int32));

            using (StreamWriter file =
    new StreamWriter(@"d:\WriteLines2.txt"))
            {
                var r = cmd.ExecuteReader();

                for (var i = 0; i < _count * _times; i++)
                {
                    // cmd.Parameters["id"].Value = i;



                    if (!r.Read()) break;

                    Byte[] d = new Byte[42];

                    var name = r.GetInt64(1);
                    var lorem = r.GetBytes(2, 0, d, 0, 42);
                    file.WriteLine(name);

                }
                r.Close();
            }
        }

        public void Delete()
        {
            var cmd = new SQLiteCommand("DELETE FROM col", _db);

            cmd.ExecuteNonQuery();
        }

        public void Drop()
        {
            var cmd = new SQLiteCommand("DROP TABLE col_bulk", _db);

            cmd.ExecuteNonQuery();
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}
