/**
 * Comments courtesy of AI!!
 * This is a data structure memory layout analyzer and optimizer.
 * It helps analyze and visualize memory alignment, padding, and potential performance issues
 * in struct-like data structures by simulating their memory layout.
 */

function padding(amount) {
    return { padding: true, size: amount, alignment: 1 };
}

/**
 * Define the basic data types with their sizes and alignment requirements
 */
const types = {
    byte: { size: 1, alignment: 1, name: "byte" },
    integer: { size: 4, alignment: 4, name: "integer" },
    long: { size: 8, alignment: 8, name: "long" }
};

types["string"] = { size: 20, alignment: 8, name: "string", accessPatterns: [types.long, types.long, types.integer] };

/**
 * Sample data structure layout to analyze
 * Contains a mix of different data types to demonstrate alignment and padding effects
 */
let columns = [types.byte, types.integer, types.long, types.byte, types.integer, types.byte, types.long, types.integer, types.byte, types.long];

// Find the largest alignment requirement and sort columns by alignment
// This helps optimize the memory layout
const columnsLargestAlignment = Math.max(...columns.map(c => c.alignment));
columns.sort((a, b) => a.alignment - b.alignment);

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
const runCount = 5;

let cacheBoundaryCross = 0;   // Count of cache line (64 bytes) boundary crossings
let misalignedReads = 0;      // Count of misaligned memory accesses
let pageBoundaryCross = 0;    // Count of page (4KB) boundary crossings

let address = 4096;
for (let i = 0; i < runCount; i++) {
    console.log(`\x1b[91mstruct\x1b[38;5;214m s${i + 1}\x1b[97m {\x1b[0m`);

    let offset = 0;
    for (let j = 0; j < columns.length; j++) {
        let accessAddress = address + offset;
        const column = columns[j];
        offset += column.size;

        if (column.padding) {
            console.log(`  \x1b[90m(${column.size} byte padding)\x1b[0m`);
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
                ++misalignedReads;
            }
    
            if (bytesIntoCacheBoundary + (actualColumn.size - 1) > 63) {
                notes.push(`\x1b[33mcrossed cache boundary ⚠️\x1b[0m  `); // remember that we read the actual byte, not skip over it, hence the -1!
                ++cacheBoundaryCross;
            }
            
            if (bytesIntoPageBoundary + (actualColumn.size - 1) > 4095) {
                notes.push(`\x1b[35mcrossed page boundary\x1b[0m`);
                ++pageBoundaryCross;
            }

            accessAddress += actualColumn.size;
        }

        console.log(`  \x1b[36m${column.name}\x1b[0m \x1b[97mm${j};\x1b[0m \x1b[90m// ${notes.join(", ")}\x1b[0m`);
    }

    console.log(`\x1b[97m};`);
    console.log();
    address += offset;
}

console.log(`Misaligned reads = ${misalignedReads}`);
console.log(`Cache boundary cross-reads = ${cacheBoundaryCross}`);
console.log(`Page boundary cross-reads = ${pageBoundaryCross}`);