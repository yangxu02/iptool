package com.linkx.tools.ip;

/**
 * Created by yangxu on 6/24/14.
 */

class IPRange {

    long base;
    int[] offsets;

    /**
     * binary search
     *
     * @param offset
     * @return
     */
    public int find(int offset) {
        int low = 0;
        int high = offsets.length - 1;  // initial partition is whole table

        if (offset < offsets[low]) {
            return 0;
        }

        if (offset > offsets[high]) { // cross two span
            // not in this region
            return -1;
        }

        boolean found = false;
        int mid = 0;
        while (!found && low <= high) {
            mid = (low + high) >>> 1;
            if (offset > offsets[mid]) {
                low = mid + 1;
            } else if (offset < offsets[mid]) {
                high = mid - 1;
            } else {
                found = true;
            }
        }
        if (found) {
            return mid;
        } else {
            // first greater than
            return low;
        }
    }
}
