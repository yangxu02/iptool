#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/unordered_map.hpp>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/trim.hpp>


#include <string>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <fstream>
#include <sstream>
#include <iostream>

#include "ip_index_helper.h"


namespace po = boost::program_options;

struct ip_data_t {
    IPRangeIndex *index;
    boost::unordered_map<uint32_t, std::string> codes;
    std::vector<std::string> codes_name;

    ~ip_data_t() {
        if (NULL != index)
            delete[] index;
    }
};


int load_ip_data(const char* file_name, ip_data_t& ip_data);
bool load(const char* fIndex, const char* fDb, ip_data_t& ip_data);
const std::string& find(const std::string& ip_str, const ip_data_t& ip_data);
int verify(const char* input, const char* output, ip_data_t& ip_data);


bool load(const char* fIndex, const char* fDb,
        ip_data_t& ip_data) {

    IPRangeIndex* index = IPRangeIndex::loadFromFile(fIndex);
    if (NULL == index) {
        std::cout << ("load ip index failed") << std::endl;
        return false;
    }
    ip_data.index = index;

    int ret = load_ip_data(fDb, ip_data);
    if (ret < 0) {
        std::cout << ("load ip data failed") << std::endl;
        return false;
    }

    return true;
}

int load_ip_data(const char* file_name, ip_data_t& ip_data) {
    if (NULL == file_name) {
        std::cout << ("bad param for load_ip_data_func") << std::endl;
        return -1;
    }

    std::ifstream ifs(file_name);
    if (!ifs) {
        std::cout << "fopen file error, file=" << file_name << std::endl;
        return -1;
    }

    ip_data.codes.clear();

    std::string line;
    uint32_t region;
    uint32_t offset;
    uint32_t nlines = 0;
    //char c;
    // region_offset:usa
    //
//const boost::algorithm::detail::is_any_ofF<char> delim = boost::is_any_of(":_");
    while (getline(ifs, line)) {

        size_t i = line.find_first_of(':');

        sscanf(line.c_str(), "%u_%u:", &region, &offset);

            /*
        std::istringstream ss(line);
        std::string code;
        ss >> region >> c >> offset >> c >> code;
        */

        std::string code = line.substr(i + 1);


        ip_data.codes[(region<<16) + offset] = code; // sorted in each region

        ++nlines;
       }

    ifs.close();
    std::cout << "finish loading ip data, lines=" << nlines << std::endl;

    return 0;
}

std::string UNKNOWN = "unknown";

std::string& find(std::string& ip_str, ip_data_t& ip_data) {

    uint32_t region  = 0;
    uint32_t offset = 0;
    int ret = (ip_data.index)->find(ip_str, &region, &offset);
    if (-1 == ret) {
        std::cout << "not found in ip index ip_str=" << ip_str.c_str() << std::endl;
        return UNKNOWN;
    }

    return ip_data.codes[(region<<16)+offset];
}


int verify(const char* input, const char* output, ip_data_t& ip_data) {
    if (NULL == input || NULL == output) {
        std::cout << ("bad param for verify func") << std::endl;
        return -1;
    }

    std::ifstream ifs(input);
    if (!ifs) {
        std::cout << "fopen file error, file=" << input << std::endl;
        return -1;
    }

    std::ofstream ofs(output);
    if (!ofs) {
        std::cout << "fopen file error, file=" << output << std::endl;
        return -1;
    }

    std::string line;
    uint32_t nlines = 0;
    //char c;
    // region_offset:usa
    while (getline(ifs, line)) {
        std::istringstream ss(line);
        std::string ip;
        std::string country;
        std::string city;
        ss >> ip >> country >> city;

        const std::string& code = find(ip, ip_data);

        ofs << ip << '\x01' << country << '\x01' << city << '\x01' << code << std::endl;

        ++nlines;
       }

    ifs.close();
    ofs.close();
    std::cout << "finish verify ip, lines=" << nlines << std::endl;

    return 0;
}

po::options_description usage("Usage");

int main(int argc, char* argv[])
{


    // Declare a group of options that will be
    // allowed only on command line
    usage.add_options()
        ("help", "produce help message")
        ("index", po::value<std::string>(), "ip index file")
        ("db", po::value<std::string>(), "ip db file")
        ("input", po::value<std::string>(), "ip list to verify")
        ("output", po::value<std::string>(), "verify result")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, usage), vm);
    po::notify(vm);


    if (!vm.count("index") || !vm.count("db")
            || !vm.count("input") || !vm.count("output")) {

        std::cout << usage << std::endl;

        return -1;

    }

    std::string indexFile = vm["index"].as<std::string>();
    std::string dbFile = vm["db"].as<std::string>();
    std::string input = vm["input"].as<std::string>();
    std::string output = vm["output"].as<std::string>();

    ip_data_t ip_data;

    load(indexFile.c_str(), dbFile.c_str(), ip_data);

    verify(input.c_str(), output.c_str(), ip_data);

    std::cout << "finish" << std::endl;

    return 0;

}

