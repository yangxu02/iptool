#ifndef __IP_INDEX_HELPER_H__
#define __IP_INDEX_HELPER_H__

#include <stdint.h>
#include <stdlib.h>
#include <string>

struct IPRange {
    uint32_t base;
    int n;
    uint16_t* offsets;

    ~IPRange() {
        if (NULL != offsets) {
            delete[] offsets;
        }
    }

    /**
     * binary search
     * @param offset
     * @return
     */
    int find(uint16_t offset) {
        int low = 0;
        int high = n - 1;  // initial partition is whole table

        if (offset < offsets[low]) {
            return 0;
        }

        if (offset > offsets[high]) { // cross two span
            // not in this region
            return -1;
        }

        bool found = false;
        int mid = 0;
        while (!found && low <= high)
        {
            mid = (low + high) >> 1;
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
};

class IPRangeIndex {
    private:
        IPRange** regions;
        uint32_t nRegions;
        uint32_t* next;

    public:
        static const uint32_t mode = 16;

        ~IPRangeIndex() {
            if (NULL != next)
                delete[] next;
            if (NULL != regions) {
                uint32_t len = sizeof(regions) / sizeof(IPRange*);
                for (uint32_t i = 0; i < len; ++i) {
                    if (NULL != regions[i])
                        delete regions[i];
                }

                delete[] regions;
            }
        }

        static IPRangeIndex* loadFromFile(const char* file);

        int find(uint32_t ip, uint32_t* iRegion, uint32_t* iOffset) {
            uint32_t region = (ip >> IPRangeIndex::mode); // right shift
            if (region >= nRegions) return -1; // region overflow
            IPRange* range = regions[region];
            int offset = -1;
            if (NULL != range) {
                offset = range->find(ip - range->base);
                if (-1 != offset) {
                    *iRegion = region;
                    *iOffset = offset;
                    return 0;
                }
            }
            *iRegion = next[region];
            *iOffset = 0;

            return 0;
        }

        int find(std::string& ip_str, uint32_t* iRegion, uint32_t* iOffset) {
            uint32_t ip = iptoi(ip_str);
            if (0 == ip) return -1;

            uint32_t region = (ip >> IPRangeIndex::mode);
            if (region >= nRegions) return -1;
            IPRange* range = regions[region];
            int offset = -1;
            if (NULL != range) {
                offset = range->find(ip - range->base);
                if (-1 != offset) {
                    *iRegion = region;
                    *iOffset = offset;
                    return 0;
                }
            }
            // begin of next region
            *iRegion = next[region];
            *iOffset = 0;

            return 0;
        }


        uint32_t iptoi(std::string& ip_str) {

            uint32_t len = ip_str.length();
            if (len == 0 || len > 15) {
                return 0;
            }
            uint32_t ip = 0;

            const char* s = ip_str.c_str();
            uint32_t seg = 0;
            char c;
            uint32_t bits = 24;
            while ((c = *s++)) {
                if (std::isdigit(c)) {
                    seg = seg * 10 + (c - '0');
                } else if (c == '.' && seg <= 255) {
                    ip += seg << bits;
                    bits -= 8;
                    seg = 0;
                } else {
                    return 0;
                }
            }
            if (seg <= 255) {
                ip += seg << bits;
                return ip;
            }
            return 0;
        }
};

#endif
