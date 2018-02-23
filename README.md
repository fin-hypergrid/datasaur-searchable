# fin-hypergrid-primary-key

Hypergrid plug-in to get/delete/modify a data row based on a unique single- or multi-column ID

These functions are all basically wrappers for the various `findRow` overloads.

## Installation

**Important:** Your data source must support the [`findRow`](https://github.com/fin-hypergrid/fin-hypergrid-find-row) function.

### Using the build files with `<script>` tags and Hypergrid's `require()`

Build files are available on the CDN for Hypergrid itself and the `primary-key` API.

In `<head>...</head>` element, include <u>one of each</u> of the following (after including Hypergrid itself):
```html
<script src="https://fin-hypergrid.github.io/core/3.0.0/build/fin-hypergrid.js"></script>
<script src="https://fin-hypergrid.github.io/core/3.0.0/build/fin-hypergrid.min.js"></script>

<script src="https://fin-hypergrid.github.io/primary-key/1.0.0/build/fin-hypergrid-primary-key.js"></script>
<script src="https://fin-hypergrid.github.io/primary-key/1.0.0/build/fin-hypergrid-primary-key.min.js"></script>
```
### Using CommonJS modules with Browserify's or Webpack's `require()`
In a `.js` file that serves as input to Browserify or Webpack:
```js
var Hypergrid = require('fin-hypergrid');
```
### Instantiate with plug-in(s):
```js
var grid = new Hypergrid({
    plugins: [
        require('fin-hypergrid-primary-key'),
        ... // more plug-ins go here
    ],
    ... // more insantiation options go here
});
```

## Usage

_Instructions:_ Create a simple Hypergrid [example](http://github.com/fin-hypergrid/demo/example.html)
that installs the plug-in (as above). Then copy & paste each of the following code blocks into Chrome's developer
console and hit the return key, observing the changes to the grid as you do so.

1. Set up some variables:
```javascript
var behavior = grid.behavior;
var dataModel = behavior.dataModel;
var findKey = "symbol";
var findVal = 'FB';
```
2. Add a new row:
```javascript
var newDataRow = {};
newDataRow[findKey] = findVal;
newDataRow.name = 'Facebook';
newDataRow.prevclose = 125.08;
dataModel.addRow(newDataRow);
// To see the new row you must (eventually) call:
behavior.reindex();
grid.behaviorChanged();
```
3. Modify an existing row:
```javascript
var modKey = 'name';
var modVal = 'Facebook, Inc.';
var dataRow = dataModel.modifyRowById(findKey, findVal, modKey, modVal);
// To see the modified cells you must (eventually) call:
behavior.reindex();
grid.repaint();
```
4. Delete (remove) a row:
```javascript
var oldRow = dataModel.deleteRowById(findKey, findVal);
// To see the row disappear you must (eventually) call:
behavior.reindex();
grid.behaviorChanged();
```
5. Replace an existing row:
```javascript
findVal = 'MSFT';
var newRow = {symbol: "ABC", name: "Google", prevclose: 666};
var oldRow = dataModel.replaceRowById(findKey, findVal, newRow);
// To see the row change you must (eventually) call:
behavior.reindex();
grid.repaint();
```
This replaces the row with the new row object, returning but otherwise discarding the old row object. That is, the new row object takes on the ordinal of the old row object. By contrast, modifyDataRow keeps the existing row object, updating it in place.
6. Fetch a row (find the row and return the row object):
```javascript
findVal = 'ABC';
var dataRow = dataModel.getRowById(findKey, findVal);
```
7. Get a row's index (find the row and return its ordinal) (for use with Hypergrid's various grid coordinate methods):
```javascript
var rowIndex = dataModel.getRowIndexById(findKey, findVal);
```
8. Erase (blank) a row:
```javascript
var oldRow = dataModel.eraseRowById(findKey, findVal);
// To see the row blank you must (eventually) call:
grid.behavior.reindex();
grid.repaint();
```

## Notes

### Updating the rendered grid

The following calls should be made sparingly as they can be expensive. The good news is that they only need to be called at the very end of a batch grid data changes.
   1. Call `grid.behavior.reindex()`. This call does nothing when the data source pipeline is empty. Otherwise, applies each data source transformations (filter, sort) in the pipeline. Needed when adding, deleting, or modifying rows.
   2. Call `grid.behaviorChanged()` when the number of rows (or columns) changes as a result of the data alteration.
   3. Call `grid.repaint()` when cells are updated in place. Note that `behaviorChanged` calls `repaint` for you so you only need to call one or the other.

### Search key hash option

Search key(s) may be provided in a single hash parameter instead of in two distinct parameters (_à la_ underscore.js)
For any of the methods above that take a search key, the first two arguments (`findkey` and `findVal`) may be replaced with a single argument `findHash`, a hash of key:value pairs, optionally followed by a 2nd argument `findList`, a _whitelist_ (an array) of which keys in `findHash` to use. These overloads allow for searching based on multiple columns:

#### Example 1
Single-column primary key:
```javascript
behavior.getRow({ symbol: 'FB' });
```

#### Example 2
Multi-column primary key (target row must match all keys):
```javascript
behavior.getRow({ symbol: 'FB', name: 'Facebook' });
```

#### Example 3
Limit the column(s) that comprise the primary key with `findList`, an array of allowable search keys:
```javascript
var findWhiteList = ['symbol'];
behavior.getRow({ symbol: 'FB', name: 'the facebook' }, findKeyList);
```
In the above example, name is ignored because it is absent from the key list. This example is therefore functionally equivalent to example 2.a.

### Modifications hash option

Field(s) to modify may be provided in a hash instead (_à la_ jQuery.js)
This overload allows for updating multiple columns of a row with a single method call:
```javascript
var modHash = { name: 'Facebook, Inc.', prevclose: 125};
dataModel.modifyRowById('symbol', 'FB', modHash);
```
The above is equivalent to the following separate calls which each update a specific field in the same row:
```javascript
dataModel.modifyRowById('symbol', 'FB', 'name', 'Facebook, Inc.');
dataModel.modifyRowById('symbol', 'FB', 'prevclose', 125);
```
Normally all included fields will be modified. As in `findList` (described above), you can limit which fields to actually modify by providing an additional parameter `modList`, an array of fields to modify. The following example modifies the "prevClose" field but not the "name" field:
```javascript
var modWhiteList = ['prevclose'];
dataModel.modifyRowById('symbol', 'FB', modHash, modWhiteList);
```

## Summary

The overloads discussed above in _Search key hash option_ and _Modifier hash option_ may be combined. For example, the full list of overloads for {@link rowById.modifyRowById} is:

```javascript
behavior.modifyRowById(findKey, findVal, modKey, modVal);
behavior.modifyRowById(findKey, findVal, modHash);
behavior.modifyRowById(findKey, findVal, modHash, modWhiteList);
behavior.modifyRowById(findHash, modKey, modVal);
behavior.modifyRowById(findHash, modHash);
behavior.modifyRowById(findHash, modHash, modWhiteList);
behavior.modifyRowById(findHash, findWhiteList, modKey, modVal);
behavior.modifyRowById(findHash, findWhiteList, modHash);
behavior.modifyRowById(findHash, findWhiteList, modHash, modWhiteList);
```
