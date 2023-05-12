<?php

namespace DanielHe4rt\Scylloquent\Schema;

use Illuminate\Support\Fluent;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\Grammar as BaseGrammar;

class Grammar extends BaseGrammar
{
    /**
     * The possible column modifiers.
     *
     * @var array
     */
    protected $modifiers = [
        'Unsigned', 'VirtualAs', 'StoredAs', 'Charset', 'Collate', 'Nullable',
        'Default', 'Increment', 'Comment', 'After', 'First', 'Srid',
    ];

    /**
     * The possible column serials.
     *
     * @var array
     */
    protected $serials = ['bigInteger', 'integer', 'mediumInteger', 'smallInteger', 'tinyInteger'];


    /**
     * Compile the query to determine the list of tables.
     */
    public function compileTableExists(): string
    {
        return "SELECT \"table_name\" "
            . "FROM \"system_schema\".\"tables\" "
            . "WHERE table_name = :\"table_name\" "
            . "AND keyspace_name = :\"keyspace_name\"";
    }

    /**
     * Compile the query to determine the list of columns.
     */
    public function compileColumnListing(): string
    {
        return "SELECT \"column_name\" "
            . "FROM \"system_schema\".\"columns\" "
            . "WHERE table_name = :\"table_name\" "
            . "AND keyspace_name = :\"keyspace_name\"";
    }

    /**
     * Compile a create table command.
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $cql = $this->compileCreateTable($blueprint, $command, $connection);
        return $this->compileCreateEngine($cql, $connection, $blueprint);
    }

    /**
     * Create the main create table clause.
     */
    protected function compileCreateTable($blueprint, $command, $connection): string
    {
        return sprintf('%s table %s (%s, %s) %s',
            'create',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint)),
            $this->compilePrimary($blueprint, $command),
            $this->compileWithOptions($blueprint, $command)
        );
    }

    /**
     * Append the engine specifications to a command.
     */
    protected function compileCreateEngine($sql, Connection $connection, Blueprint $blueprint): string
    {
        if (isset($blueprint->engine)) {
            return $sql . ' engine = ' . $blueprint->engine;
        }

        if (!is_null($engine = $connection->getConfig('engine'))) {
            return $sql . ' engine = ' . $engine;
        }

        return $sql;
    }

    /**
     * Compile an add column command.
     */
    public function compileAdd(Blueprint $blueprint, Fluent $command): string
    {
        $columns = $this->prefixArray('add', $this->getColumns($blueprint));

        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a primary key command.
     */
    public function compilePrimary(Blueprint $blueprint, Fluent $command)
    {
        return $blueprint->compilePrimary();
    }

    /**
     * Compile with options.
     */
    public function compileWithOptions(Blueprint $blueprint, Fluent $command): string
    {
        return $blueprint->compileWithOptions();
    }

    /**
     * Compile a unique key command.
     */
    public function compileUnique(Blueprint $blueprint, Fluent $command): string
    {
        return $this->compileKey($blueprint, $command, 'unique');
    }

    /**
     * Compile a plain index key command.
     */
    public function compileIndex(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'CREATE INDEX %s ON %s (%s)',
            $this->wrap($command->index),
            $this->wrapTable($blueprint),
            $this->columnize($command->columns)
        );
    }

    /**
     * Compile an index creation command.
     */
    protected function compileKey(Blueprint $blueprint, Fluent $command, string $type): string
    {
        return sprintf('alter table %s add %s %s%s(%s)',
            $this->wrapTable($blueprint),
            $type,
            $this->wrap($command->index),
            $command->algorithm ? ' using ' . $command->algorithm : '',
            $this->columnize($command->columns)
        );
    }

    /**
     * Compile a drop table command.
     */
    public function compileDrop(Blueprint $blueprint, Fluent $command): string
    {
        return 'drop table ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop by table name.
     */
    public function compileDropTable(string $table): string
    {
        return 'drop table ' . $this->wrapTable($table);
    }

    /**
     * Compile a drop table (if exists) command.
     */
    public function compileDropIfExists(Blueprint $blueprint, Fluent $command): string
    {
        return 'drop table if exists ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop by table name.
     */
    public function compileDropTableIfExists(string $table): string
    {
        return 'drop table if exists ' . $this->wrapTable($table);
    }

    /**
     * Compile a drop column command.
     */
    public function compileDropColumn(Blueprint $blueprint, Fluent $command): string
    {
        $columns = $this->prefixArray('drop', $this->wrapArray($command->columns));

        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a drop primary key command.
     */
    public function compileDropPrimary(Blueprint $blueprint, Fluent $command): string
    {
        return 'alter table ' . $this->wrapTable($blueprint) . ' drop primary key';
    }

    /**
     * Compile a drop unique key command.
     */
    public function compileDropUnique(Blueprint $blueprint, Fluent $command): string
    {
        $index = $this->wrap($command->index);

        return "alter table {$this->wrapTable($blueprint)} drop index {$index}";
    }

    /**
     * Compile a drop index command.
     */
    public function compileDropIndex(Blueprint $blueprint, Fluent $command): string
    {
        $index = $this->wrap($command->index);

        return "alter table {$this->wrapTable($blueprint)} drop index {$index}";
    }

    /**
     * Compile a rename table command.
     */
    public function compileRename(Blueprint $blueprint, Fluent $command): string
    {
        $from = $this->wrapTable($blueprint);

        return "rename table {$from} to " . $this->wrapTable($command->to);
    }

    /**
     * Compile a rename index command.
     */
    public function compileRenameIndex(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf('alter table %s rename index %s to %s',
            $this->wrapTable($blueprint),
            $this->wrap($command->from),
            $this->wrap($command->to)
        );
    }

    /**
     * Compile the SQL needed to drop all tables.
     */
    public function compileDropAllTables(array $tables): string
    {
        return 'drop table ' . implode(',', $this->wrapArray($tables));
    }

    /**
     * Compile the SQL needed to retrieve all table names.
     */
    public function compileGetAllTables(): string
    {
        return "SELECT \"table_name\" "
            . "FROM \"system_schema\".\"tables\" "
            . "WHERE keyspace_name = :\"keyspace_name\"";
    }

    /**
     * Create the column definition for a inet type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeInet(Fluent $column): string
    {
        return 'inet';
    }

    /**
     * Create the column definition for an int type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeInt(Fluent $column): string
    {
        return 'int';
    }

    /**
     * Create the column definition for an integer type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeInteger(Fluent $column): string
    {
        return 'int';
    }

    /**
     * Create the column definition for a big integer type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeBigInteger(Fluent $column): string
    {
        return 'bigint';
    }

    /**
     * Create the column definition for a tiny integer type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTinyInteger(Fluent $column): string
    {
        return 'tinyint';
    }

    /**
     * Create the column definition for a small integer type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeSmallInteger(Fluent $column): string
    {
        return 'smallint';
    }

    /**
     * Create the column definition for a float type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeFloat(Fluent $column): string
    {
        return $this->typeDouble($column);
    }

    /**
     * Create the column definition for a double type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeDouble(Fluent $column): string
    {
        if ($column->total && $column->places) {
            return "double({$column->total}, {$column->places})";
        }

        return 'double';
    }

    /**
     * Create the column definition for a decimal type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeDecimal(Fluent $column): string
    {
        return "decimal({$column->total}, {$column->places})";
    }

    /**
     * Create the column definition for a boolean type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeBoolean(Fluent $column): string
    {
        return 'boolean';
    }

    /**
     * Create the column definition for a string type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeString(Fluent $column): string
    {
        return "varchar";
    }

    /**
     * Create the column definition for a text type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeText(Fluent $column): string
    {
        return 'text';
    }

    /**
     * Create the column definition for a date type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeDate(Fluent $column): string
    {
        return 'date';
    }

    /**
     * Create the column definition for a date-time type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeDateTime(Fluent $column): string
    {
        return 'datetime';
    }

    /**
     * Create the column definition for a date-time type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeDateTimeTz(Fluent $column): string
    {
        return 'datetime';
    }

    /**
     * Create the column definition for a time type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTime(Fluent $column): string
    {
        return 'time';
    }

    /**
     * Create the column definition for a time type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTimeTz(Fluent $column): string
    {
        return 'time';
    }

    /**
     * Create the column definition for a timestamp type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTimestamp(Fluent $column): string
    {
        if ($column->useCurrent) {
            return 'timestamp default CURRENT_TIMESTAMP';
        }
        return 'timestamp';
    }

    /**
     * Create the column definition for a timestamp type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTimestampTz(Fluent $column): string
    {
        if ($column->useCurrent) {
            return 'timestamp default CURRENT_TIMESTAMP';
        }
        return 'timestamp';
    }

    /**
     * Create the column definition for a binary type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeBinary(Fluent $column): string
    {
        return 'blob';
    }

    /**
     * Create the column definition for a blob type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeBlob(Fluent $column): string
    {
        return 'blob';
    }

    /**
     * Create the column definition for a uuid type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeUuid(Fluent $column): string
    {
        return 'uuid';
    }

    /**
     * Create the column definition for a list type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeList(Fluent $column): string
    {
        return 'list<' . $column->collectionType . '>';
    }

    /**
     * Create the column definition for a map type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeMap(Fluent $column): string
    {
        return 'map<' . $column->collectionType1 . ', ' . $column->collectionType2 . '>';
    }

    /**
     * Create the column definition for a set type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeSet(Fluent $column): string
    {
        return 'set<' . $column->collectionType . '>';
    }

    /**
     * Create the column definition for a timeuuid type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTimeuuid(Fluent $column): string
    {
        return 'timeuuid';
    }

    /**
     * Create the column definition for a tuple type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeTuple(Fluent $column): string
    {
        return 'tuple<' . $column->tuple1type . ', ' . $column->tuple2type . ', ' . $column->tuple3type . '>';
    }

    /**
     * Create the column definition for a counter type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeCounter(Fluent $column): string
    {
        return 'counter';
    }

    /**
     * Create the column definition for a frozen type.
     *
     * @param \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function typeFrozen(Fluent $column): string
    {
        return 'frozen';
    }

    /**
     * Get the SQL for a generated virtual column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyVirtualAs(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->virtualAs)) {
            return " as ({$column->virtualAs})";
        }

        return '';
    }

    /**
     * Get the SQL for a generated stored column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyStoredAs(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->storedAs)) {
            return " as ({$column->storedAs}) stored";
        }

        return '';
    }

    /**
     * Get the SQL for an unsigned column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyUnsigned(Blueprint $blueprint, Fluent $column): string
    {
        if ($column->unsigned) {
            return ' unsigned';
        }

        return '';
    }

    /**
     * Get the SQL for a character set column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyCharset(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->charset)) {
            return ' character set ' . $column->charset;
        }

        return '';
    }

    /**
     * Get the SQL for a collation column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyCollate(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->collation)) {
            return ' collate ' . $column->collation;
        }

        return '';
    }

    /**
     * Get the SQL for a nullable column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyNullable(Blueprint $blueprint, Fluent $column): string
    {
        if (is_null($column->virtualAs) && is_null($column->storedAs)) {
            return $column->nullable ? ' null' : ' not null';
        }

        return '';
    }

    /**
     * Get the SQL for a default column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyDefault(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->default)) {
            return ' default ' . $this->getDefaultValue($column->default);
        }

        return '';
    }

    /**
     * Get the SQL for an auto-increment column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyIncrement(Blueprint $blueprint, Fluent $column): string
    {
        if (in_array($column->type, $this->serials) && $column->autoIncrement) {
            return ' auto_increment primary key';
        }

        return '';
    }

    /**
     * Get the SQL for a "first" column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyFirst(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->first)) {
            return ' first';
        }

        return '';
    }

    /**
     * Get the SQL for an "after" column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyAfter(Blueprint $blueprint, Fluent $column): ?string
    {
        if (!is_null($column->after)) {
            return ' after ' . $this->wrap($column->after);
        }

        return '';
    }

    /**
     * Get the SQL for a "comment" column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent $column
     * @return string|null
     */
    protected function modifyComment(Blueprint $blueprint, Fluent $column): string
    {
        if (!is_null($column->comment)) {
            return " comment '" . $column->comment . "'";
        }

        return '';
    }

    /**
     * Compile the blueprint's column definitions.
     */
    protected function getColumns(Blueprint $blueprint): array
    {
        $columns = [];

        foreach ($blueprint->getAddedColumns() as $column) {
            // Each of the column types have their own compiler functions which are tasked
            // with turning the column definition into its SQL format for this platform
            // used by the connection. The column's modifiers are compiled and added.
            $sql = $this->wrap($column) . ' ' . $this->getType($column);
            $columns[] = $sql;
        }

        return $columns;
    }
}
