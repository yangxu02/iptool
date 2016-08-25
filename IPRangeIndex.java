package com.linkx.tools.ip;
/**
 * Created by yangxu on 6/24/14.
 */

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

class IPRangeIndex {
    private static Logger logger = Logger.getLogger(IPRangeIndex.class);
    final static int mode = 16;
    IPRange[] regions;
    int[] next;

    public static IPRangeIndex loadFromFile(String file) {
        IPRangeIndex ipRangeIndex = new IPRangeIndex();
        ipRangeIndex.regions = new IPRange[1 << mode];

        InputStreamReader fileReader;
        BufferedReader reader = null;
        FileInputStream stream = null;
//        List<Integer> regionsUsed = new ArrayList<Integer>();
        try {
            stream = new FileInputStream(file);
            fileReader = new InputStreamReader(stream, "UTF-8");

            reader = new BufferedReader(fileReader);
            long lineNumber = 0;
            String inLine;
            int nRange = 0;
            while (null != (inLine = reader.readLine())) {
                lineNumber++;
                if ((lineNumber % 10000) == 0)
                    logger.info("loading " + lineNumber);
                if (inLine.isEmpty()) continue; // skip empty line, in case
                String[] values = inLine.split(":");
                int region = Integer.parseInt(values[0]);
                int size = Integer.parseInt(values[1]);
                long base = Long.parseLong(values[2]);

                IPRange range = ipRangeIndex.regions[region];
                if (null == range) {
                    range = new IPRange();
                    range.base = base;
                    range.offsets = new int[size];
                    nRange = 0;
                    ipRangeIndex.regions[region] = range;
//                    regionsUsed.add(region);
                }

                inLine = reader.readLine();
                if (null == inLine || inLine.isEmpty()) {
                    logger.error("index file cruptted");
                    return null;
                }
                values = inLine.split(",");
                for (String value : values) {
                    range.offsets[nRange++] = Integer.parseInt(value);
                }
            }

            int[] next = new int[ipRangeIndex.regions.length];
            int s = 0;
            int e = 0;
            while (e != ipRangeIndex.regions.length) {
                while (null == ipRangeIndex.regions[e]) {
                    ++e;
                }
                while (s < e) {
                    next[s++] = e;
                }
                ++e;
            }
            ipRangeIndex.next = next;
            return ipRangeIndex;
        } catch (Exception e) {
            logger.error("", e);
            return null;
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                    stream.close();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
        }
    }

    public String find(long ip) {
        int iRegion = (int) (ip >> IPRangeIndex.mode);
        if (iRegion >= regions.length) return null;
        IPRange range = regions[iRegion];
        if (null == range) {
            return (next[iRegion] + "_0");
        }
        int iOffset = range.find((int) (ip - range.base));
        if (-1 == iOffset) {
            return (next[iRegion] + "_0");
        }
        return (iRegion + "_" + iOffset);
    }
}
