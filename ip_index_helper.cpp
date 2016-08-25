#include <sstream>
#include <fstream>
#include <iostream>
#include <cstdio>
#include "ip_index_helper.h"

IPRangeIndex* IPRangeIndex::loadFromFile(const char* file) {
    IPRangeIndex* ipRangeIndex = new IPRangeIndex();
    ipRangeIndex->nRegions = 1 << mode;
    ipRangeIndex->regions = new IPRange* [ipRangeIndex->nRegions];

    std::ifstream ifs(file);
    std::string line;
    if (ifs) {
        uint32_t lineNumber = 0;
        uint32_t nRange = 0;
        uint32_t region;
        uint32_t size;
        uint32_t base;
        //                uint16_t seg = 0;

        while (getline(ifs, line)) {
            lineNumber++;
            if ((lineNumber % 10000) == 0)
                std::cout << "loading " << lineNumber << std::endl;
            if (0 == line.length()) continue; // skip empty line, in case
            sscanf(line.c_str(), "%u:%u:%u", &region, &size, &base);

            IPRange* range = ipRangeIndex->regions[region];
            if (NULL == range) {
                range = new IPRange();
                range->base = base;
                range->offsets = new uint16_t[size];
                range->n = size;
                nRange = 0;
                ipRangeIndex->regions[region] = range;
            }

            if (getline(ifs, line) && 0 != line.length()) {
                //                        UI_DEBUG("index offset region=%u, nRange=%u, size=%u, line:%s", region, nRange, size, line.c_str());
                std::istringstream ss(line);
                char c;
                ss >> range->offsets[nRange]; // nRange == 0
                for (nRange = 1; nRange < size; ++nRange) {
                    ss >> c >> range->offsets[nRange];
                }
                /*
                   UI_DEBUG("region=%u, offset[0]=%u, offset[-1]=%u, nRange=%u",
                   region, range->offsets[0],
                   range->offsets[nRange-1],
                   nRange);
                   */
            } else {
                std::cout << "index file cruptted" << std::endl;
                return NULL;
            }
        }

        uint32_t len = ipRangeIndex->nRegions;
        uint32_t* next = new uint32_t[len];
        uint32_t s = 0;
        uint32_t e = 0;
        while (e != len) {
            while (NULL == ipRangeIndex->regions[e]) {
                ++e;
            }
            while (s < e) {
                next[s++] = e;
            }
            ++e;
        }
        ipRangeIndex->next = next;
        ifs.close();
        std::cout << "finish loading ip index lineNumber=" << lineNumber << std::endl;
        return ipRangeIndex;
    } else {
        std::cout << "open ip index file failed" << std::endl;
        return NULL;
    }
}

