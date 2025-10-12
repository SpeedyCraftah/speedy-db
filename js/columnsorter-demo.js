/**
 * Comments courtesy of LLMs!!
 * This is a data structure memory layout analyzer and optimizer.
 * It helps analyze and visualize memory alignment, padding, and potential performance issues
 * in struct-like data structures by simulating their memory layout.
 * This was designed to test and trial out different ideas before implementing the actual column optimizer which is in the database now.
 */

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const value = bytes / Math.pow(k, i);
  return `${value.toFixed(1)} ${sizes[i]}`;
}

function padding(amount) {
    return { padding: true, size: amount, alignment: 1 };
}

function _findCombination(bestCombo, numbers, indexes, target, getNum) {
    for (const [i, num] of numbers.entries()) {
        const resultingSet = [...indexes, num];
        const count = resultingSet.reduce((acc, curr) => acc + getNum(curr), 0);
        if (count > target) continue; 

        if (bestCombo.value === null) bestCombo.value = resultingSet;
        else {
            const bestComboCount = bestCombo.value.reduce((acc, curr) => acc + getNum(curr), 0);
            if (bestComboCount !== target) {
                if (count === target) bestCombo.value = resultingSet;
                else if (count > bestComboCount) bestCombo.value = resultingSet;
                else if (count >= bestComboCount && resultingSet.length < bestCombo.value.length) bestCombo.value = resultingSet;
            }
            else if (count === target && resultingSet.length < bestCombo.value.length) bestCombo.value = resultingSet;
        }
        
        _findCombination(bestCombo, numbers.slice(i + 1), [...resultingSet], target, getNum);
    }

    return bestCombo;
}

function findShortestCombination(numbers, target, getNum) {
    return _findCombination({ value: null }, numbers, [], target, getNum)?.value;
}

function printRecord(name, columns, address = 4096, saveMisaligned = false) {
    console.log(`\x1b[91mstruct\x1b[38;5;214m ${name}\x1b[97m {\x1b[0m`);

    let offset = 0;
    for (let j = 0; j < columns.length; j++) {
        let accessAddress = address + offset;
        const column = columns[j];
        offset += column.size;

        if (column.padding) {
            console.log(`  \x1b[90m// (${column.size} byte padding)\x1b[0m`);
            continue;
        }

        const notes = [];
        notes.push(`accessed at ${accessAddress}`);

        for (const actualColumn of column.accessPatterns ? column.accessPatterns : [column]) {
            const bytesIntoCacheBoundary = accessAddress % 64;
            const bytesIntoPageBoundary = accessAddress % 4096;
            
            if ((accessAddress % actualColumn.alignment) === 0) {
                notes.push(`\x1b[32maligned${column.accessPatterns ? ` (${actualColumn.name})` : ''}\x1b[0m`);
            } else {
                notes.push(`\x1b[31mmisaligned${column.accessPatterns ? ` (${actualColumn.name})` : ''} (${accessAddress % actualColumn.alignment} byte${(accessAddress % actualColumn.alignment) === 1 ? '' : 's'} off)\x1b[0m`);
                if (saveMisaligned) ++misalignedReads;
            }
    
            if (bytesIntoCacheBoundary + (actualColumn.size - 1) > 63) {
                notes.push(`\x1b[33mcrossed cache boundary ⚠️\x1b[0m  `); // remember that we read the actual byte, not skip over it, hence the -1!
                if (saveMisaligned) ++cacheBoundaryCross;
            }
            
            if (bytesIntoPageBoundary + (actualColumn.size - 1) > 4095) {
                notes.push(`\x1b[35mcrossed page boundary\x1b[0m`);
                if (saveMisaligned) ++pageBoundaryCross;
            }

            accessAddress += actualColumn.size;
        }

        console.log(`  \x1b[36m${column.name}\x1b[0m \x1b[97mm${j};\x1b[0m \x1b[90m// ${notes.join(", ")}\x1b[0m`);
    }

    console.log(`\x1b[97m};`);
    console.log();

    return offset;
}

/**
 * Define the basic data types with their sizes and alignment requirements
 */
const types = {
    byte: { size: 1, alignment: 1, name: "byte" },
    integer: { size: 4, alignment: 4, name: "integer" },
    float: { size: 4, alignment: 4, name: "float" },
    long: { size: 8, alignment: 8, name: "long" }
};

types["string"] = { size: 20, alignment: 8, name: "string", accessPatterns: [types.long, types.long, types.integer] };

/**
 * Sample data structure layout to analyze
 * Contains a mix of different data types to demonstrate alignment and padding effects
 */
let columns = JSON.parse(JSON.stringify([types.byte, types.byte, types.byte, types.byte, types.byte, types.string, types.float, types.long, types.string, types.float, types.long, types.string, types.float, types.long, types.string, types.float, types.long, types.string, types.float, types.long]));
printRecord("original", columns, 4096);

let originalPaddingNeeded = 0;
let misalignedCount = 0;
let statsOffset = 0;
for (const column of columns) {
    if (statsOffset % column.alignment !== 0) {
        const paddingNeeded = column.alignment - (statsOffset % column.alignment);
        originalPaddingNeeded += paddingNeeded;
        ++misalignedCount;
        statsOffset += paddingNeeded;
    } 

    statsOffset += column.size;
}
originalPaddingNeeded += statsOffset % Math.max(...columns.map(c => c.alignment)) !== 0 ? Math.max(...columns.map(c => c.alignment)) - (statsOffset % Math.max(...columns.map(c => c.alignment))) : 0;

// Find the largest alignment requirement and sort columns by alignment
// This helps optimize the memory layout
const columnsLargestAlignment = Math.max(...columns.map(c => c.alignment));
columns.sort((a, b) => a.alignment - b.alignment);

let run1PaddingNeeded = 0;
statsOffset = 0;
for (const column of columns) {
    if (statsOffset % column.alignment !== 0) {
        const paddingNeeded = column.alignment - (statsOffset % column.alignment);
        run1PaddingNeeded += paddingNeeded;
        statsOffset += paddingNeeded;
    } 

    statsOffset += column.size;
}
run1PaddingNeeded += statsOffset % Math.max(...columns.map(c => c.alignment)) !== 0 ? Math.max(...columns.map(c => c.alignment)) - (statsOffset % Math.max(...columns.map(c => c.alignment))) : 0;

// At this point we don't need to worry about naturally aligned types being misaligned since those can be resolved with just a bit of padding, but we are very worried about misaligned types (e.g. string).
for (let i = columns.length - 1; i >= 0; --i) {
    const column = columns[i];
    const paddingNeeded = column.size % column.alignment;
    if (column.resolved || paddingNeeded === 0) continue;

    const bestCombo = findShortestCombination(columns.filter(c => c !== column && !c.resolved), paddingNeeded, v => v.size);
    if (!bestCombo) continue;

    for (const [j, paddingColumn] of bestCombo.entries()) {
        columns.splice(columns.findIndex(c => c === paddingColumn), 1);
        columns.splice(columns.findIndex(c => c === column) + j + 1, 0, paddingColumn);
        paddingColumn.resolved = true;
    }

    column.resolved = true;
    i = columns.length - 1;
}

let run2PaddingNeeded = 0;
statsOffset = 0;
for (const column of columns) {
    if (statsOffset % column.alignment !== 0) {
        const paddingNeeded = column.alignment - (statsOffset % column.alignment);
        run2PaddingNeeded += paddingNeeded;
        statsOffset += paddingNeeded;
    } 

    statsOffset += column.size;
}
run2PaddingNeeded += statsOffset % Math.max(...columns.map(c => c.alignment)) !== 0 ? Math.max(...columns.map(c => c.alignment)) - (statsOffset % Math.max(...columns.map(c => c.alignment))) : 0;

let newColumns = [];
let _offset = 0;
for (let i = 0; i < columns.length; i++) {
    let column = columns[i];
    if (_offset % column.alignment !== 0) {
        const paddingNeeded = column.alignment - (_offset % column.alignment);
        newColumns.push(padding(paddingNeeded));
        _offset += paddingNeeded;
    }

    newColumns.push(column);

    _offset += column.size;
}

const structSize = newColumns.reduce((acc, c) => acc + c.size, 0);
if (structSize % columnsLargestAlignment !== 0) {
    newColumns.push(padding(columnsLargestAlignment - (structSize % columnsLargestAlignment)));
}

columns = newColumns;

/**
 * Simulation parameters and performance counters
 * Analyzes multiple instances of the structure to detect:
 * - Misaligned memory accesses
 * - Cache line boundary crossings
 * - Page boundary crossings
 */
const runCount = 1;

let cacheBoundaryCross = 0;   // Count of cache line (64 bytes) boundary crossings
let misalignedReads = 0;      // Count of misaligned memory accesses
let pageBoundaryCross = 0;    // Count of page (4KB) boundary crossings

let address = 4096;
for (let i = 0; i < runCount; i++) {
    address += printRecord(`r${i + 1}`, columns, address, true);
}

console.log(`==== Access Statistics ====`);
console.log(`Misaligned reads = ${misalignedReads}`);
console.log(`Cache boundary cross-reads = ${cacheBoundaryCross}`);
console.log(`Page boundary cross-reads = ${pageBoundaryCross}`);

console.log();
console.log(`==== Record Layout Optimizer Statistics ====`);
console.log(`Original layout padding count = ${originalPaddingNeeded} byte${originalPaddingNeeded === 1 ? '' : 's'}`);
console.log(`Optimizer step 1 padding (quick sort) = ${run1PaddingNeeded} byte${run1PaddingNeeded === 1 ? '' : 's'}`);
console.log(`Optimizer step 2 padding (column reordering) = ${run2PaddingNeeded} byte${run2PaddingNeeded === 1 ? '' : 's'}`);

const totalSavings = originalPaddingNeeded - run2PaddingNeeded;
if (totalSavings === 0) console.log(`Optimizer savings = none; padding reduction here is impossible`);
else console.log(`Optimizer savings = ${totalSavings} byte${totalSavings === 1 ? '' : 's'} eliminated (thats ${formatBytes(totalSavings * 100000)} saved over 100,000 records)`);