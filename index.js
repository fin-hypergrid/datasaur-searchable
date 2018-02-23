'use strict';

/*
 * Glossary:
 *    PK - Primary key, an ordered list of field names comprising primary key. May be given in `options.PK` or otherwise derived as needed from search arg.
 *    PX - Primary key index, map of row indexes representing ascending sort of rows per primary key. Derived as needed from data and PK.
 */

/* eslint-env commonjs */

'use strict';

var Base = require('datasaur-base');

/**
 * @constructor
 */
var DataSourceSearchable = Base.extend('DataSourceSearchable', {
    initialize: function(dataSource, options) {
        var primaryKey = options && options.primaryKey,
            suffix = typeof primaryKey === 'object' && primaryKey.name || typeof primaryKey === 'string' && primaryKey,
            PK = typeof primaryKey === 'object' && primaryKey.columns || typeof primaryKey === 'string' && [primaryKey];

        if (suffix) {
            suffix = 'By' + suffix[0].toUpperCase() + suffix.slice(1);

            Object.keys(DataSourceSearchable.prototype)
                .filter(function(key) {
                    return key !== 'initialize' && key !== 'constructor';
                })
                .forEach(function(key) {
                    this[key + suffix] = this[key];
                }, this);

            this.install(this);
        }

        this.PK = PK;
    },

    // If called without args, simply deletes `this.PX` and returns `undefined`.
    // Otherwise, finds row and returns:
    // * row object if found
    // * `undefined` if not found
    findRow: function(sarg, options) {
        if (!arguments.length) {
            delete this.PX;
        } else {
            return find.call(this, sarg, options).dataRow;
        }
    },

    findRowIndex: function(sarg, options) {
        var index = find.call(this, sarg, options).PX_index;
        if (this.PX) {
            index = this.PX[index];
        }
        return index;
    },

    // Inserts new row and updates PX.
    // Throws error if row already exists
    // Returns:
    // * `true` if handled
    // * `false` if not handled (i.e., data source does not know how to add rows) (data and index untouched)
    insertRow: function(dataRow, options) {
        var row = find.call(this, dataRow, options);

        if (row.dataRow) {
            throw new Error('Row exists.');
        }

        var response = this.publish('add-row', dataRow),
            handled = response.length !== 0;

        if (handled && this.PX) {
            this.PX.splice(row.PX_index, 0, this.getRowCount() - 1);
        }

        return handled;
    },

    // finds row, deletes it, updates PX, and returns:
    // * deleted row object if found and handled
    // * `undefined` if not found (data and index untouched)
    // * `false` if found but not handled (i.e., data source does not know how to delete rows) (data and index untouched)
    deleteRow: function(sarg, options) {
        var row = find.call(this, sarg, options),
            result = row.dataRow;

        if (result) {
            var response = this.publish('del-row', this.PX[row.PX_index]),
                handled = response.length !== 0;

            if (!handled) {
                result = false;
            } else if (this.PX) {
                this.PX.splice(row.PX_index, 1);
            }
        }

        return result;
    }
});

/**
 * 1. Define PK with options.PK OR use previous definition OR derive it based on sarg.
 * 2. Don't use PX if `options.presorted` truthy; else derive if `options.PK` given OR `options.reindex` truthy OR not previously defined; else use reuse previous definition.
 * 3. Return an object containing:
 * * if found: `PX_index` (number) and `dataRow` (object)
 * * if not found: just `PX_index` (number) which is the insertion point
 * @param {object|string|number} sarg - An object that fully and uniquely describes the row being sought.
 * As a convenience feature for single-column primary keys, `sarg` may be a primitive value for that column.
 * @param options
 * @returns {{PX_index, dataRow}|{PX_index}}
 */
function find(sarg, options) {
    options = options || {};

    var PK = this.PK = derivePK.call(this, sarg);

    if (options.presorted) {
        delete this.PX;
    } else {
        if (options.reindex) {
            this.PX = undefined;
        }
        this.PX = derivePX.call(this);
    }

    if (typeof sarg === 'object') {
        sarg = PK.map(function (key) {
            if (!(key in sarg)) {
                throw new Error('Expected primary key column "' + key + '" to be part of search arg.');
            }
            return sarg[key];
        });
    } else if (PK.length === 1) {
        sarg = [sarg];
    } else {
        throw new Error('Expected search arg to be an object for multi-column primary key.');
    }

    if (sarg.length !== PK.length) {
        throw new Error('Expected fully qualified search arg.');
    }

    var min = 0, max = this.getRowCount() - 1;
    var maxKey = PK.length - 1;
    var getRow = this.PX ? getIndexedRow.bind(this) : this.getRow.bind(this);

    PK.slice(0, maxKey).forEach(function(key, i) {
        min = binSearchMin(getRow, key, sarg[i], min, max);
        max = binSearchMax(getRow, key, sarg[i], min, max) - 1;
    });

    return binSearch(getRow, PK[maxKey], sarg[maxKey], min, max);
}

function derivePK(sarg) {
    if (this.PK && this.PK.length) {
        return this.PK;
    }

    if (typeof sarg !== 'object') {
        throw new Error('Cannot derive primary key. Provide search key as object (or define options.PK).)')
    }

    return Object.keys(sarg)
        // map PK column name string[] to {key:string,hits:number}[]
        .map(function(key) {
            var s = {};
            if ('data' in this) {
                // only build unique value histogram when local data source (i.e., when `this.data` is available)
                this.data.forEach(function(dataRow) { s[dataRow[key]] = true; });
            }
            return { key: key, uniqueValues: Object.keys(s).length };
        }, this)
        // make columns with more hits higher order so search zooms in quicker
        .sort(function(a, b) {
            return b.uniqueValues - a.uniqueValues;
        })
        // get column names
        .map(function(o) {
            return o.key;
        });
}

function derivePX() {
    if (this.PX && this.PX.length) {
        return this.PX;
    }

    var PX = Array(this.getRowCount());

    for (var i = PX.length; i--;) {
        PX[i] = i;
    }

    return PX.sort(comparator.bind(this));
}

function comparator(a, b) {
    var result;
    a = this.getRow(a);
    b = this.getRow(b);
    this.PK.find(function(key) {
        var p = a[key], q = b[key];
        return result = p < q ? -1 : p > q ? 1 : 0;
    });
    return result;
}

function getID(rowIndex) {
    var dataRow = this.getRow(rowIndex);
    return this.PK.map(function(key) { return dataRow[key]; })
}

function getIndexedRow(indexedRowIndex) {
    return this.getRow(this.PX[indexedRowIndex]);
}

function binSearch(getRow, key, value, min, max) {
    while (min <= max) {
        var mid = Math.floor((min + max) / 2);
        var dataRow = getRow(mid);
        var field = dataRow[key];
        if (field > value) {
            max = mid - 1;
        } else if (field < value) {
            min = mid + 1;
        } else {
            // found
            return {
                PX_index: mid,
                dataRow: dataRow
            };
        }
    }
    // not found; return insertion point
    return { PX_index: min };
}

function binSearchMin(getRow, key, value, min, max) {
    while (min <= max) {
        var mid = Math.floor((min + max) / 2);
        if (getRow(mid)[key] >= value) {
            max = mid - 1;
        } else {
            min = mid + 1;
        }
    }
    return min;
}

function binSearchMax(getRow, key, value, min, max) {
    while (min <= max) {
        var mid = Math.floor((min + max) / 2);
        if (getRow(mid)[key] > value) {
            max = mid - 1;
        } else {
            min = mid + 1;
        }
    }
    return min;
}

module.exports = DataSourceSearchable;
