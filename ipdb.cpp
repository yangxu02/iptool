#include "IndexUnit.h"
#include <vector>
#include <map>
#include <string>
#include <fstream>
#include <iostream>
#include <algorithm>


#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/fusion/include/std_pair.hpp>


#include <boost/ref.hpp>

#include "redisclient.h"

#define XXXX "__Yeahmobi_Mana__"

namespace po = boost::program_options;

namespace yeahmobi {

    const boost::algorithm::detail::is_any_ofF<char> isQuote = boost::is_any_of("\"");
    const boost::algorithm::detail::is_any_ofF<char> isComma = boost::is_any_of(":");
    const boost::algorithm::detail::is_any_ofF<char> isDouhao = boost::is_any_of(",");
    const boost::algorithm::detail::is_any_ofF<char> isTab = boost::is_any_of("\t");

    struct Field {
        size_t m_index;
        size_t m_order;
        int m_dump;
        int m_save;
        int m_save_type;
        std::string m_name;
        std::string m_indexFile;

        std::string m_value;
        std::map<std::string, std::pair<size_t, std::string>> m_indexMap;
        size_t m_max;
        size_t m_seq;

        Field() {
            m_max = 0;
            m_seq = 0;
        }

        Field(size_t index, size_t order,
                std::string name, std::string indexFile) {
            m_index = index;
            m_order = order;
            m_name = name;
            m_indexFile = indexFile;
            m_max = 0;
            m_seq = 0;
            m_dump = 1;
            m_save = 1;
            m_save_type = 0;
        }

        void init(std::string input) {
            std::vector<std::string> comps;
            boost::split(comps, input, isComma);
            if (comps.size() == 7) {
                m_index = boost::lexical_cast<size_t>(comps[0]);
                m_order = boost::lexical_cast<size_t>(comps[1]);
                m_save = boost::lexical_cast<int>(comps[2]);
                m_save_type = boost::lexical_cast<int>(comps[3]);
                m_dump = boost::lexical_cast<int>(comps[4]);
                m_name = comps[5];
                m_indexFile = comps[6];
                m_max = 0;
                m_seq = 0;
            }

            if (m_dump) {
            }
        }

        void index() {
            if (m_dump) {
                //if (0 == (m_seq = m_indexMap[m_value])) {
                std::string key = boost::algorithm::to_upper_copy(m_value);
                if (0 == m_indexMap.count(key)) {
                    ++m_max;
                    m_seq = m_max;
                    m_indexMap[key] = std::pair<size_t, std::string>(m_seq, m_value);
                } else {
                    m_seq = m_indexMap[key].first;
                }
            }
        }


        void load(std::string dir) {
            if (m_dump == 0) return;
            std::cout << "load dict from " << dir << "/" << m_indexFile << std::endl;
            std::ifstream fIndex((dir + "/" + m_indexFile).c_str());
            std::string line;
            std::vector<std::string> values;
            size_t maxId = 0;
            while (getline(fIndex, line)) {
                boost::split(values, line, isTab);
                std::string key = boost::algorithm::to_upper_copy(values[1]);
                size_t id = boost::lexical_cast<size_t>(values[0]);
                m_indexMap[key] = std::pair<size_t, std::string>(id ,values[1]);
                maxId = std::max(maxId, id);
                //m_indexMap[values[1]] = boost::lexical_cast<size_t>(values[0]);
            }
            m_max = maxId;

        }

        void dump() {
            if (m_indexMap.empty() || m_dump == 0) return;
            /*
            std::vector<std::string> indexArray;
            for (size_t i = 0; i < m_indexMap.size(); ++i) {
                indexArray.push_back("");
            }

            for (std::map<std::string, size_t>::const_iterator it = m_indexMap.begin();
                    it != m_indexMap.end(); ++it) {
                indexArray[it->second] = it->first;
            }
            std::ofstream fIndex(m_indexFile.c_str());
            for (size_t i = 0; i < indexArray.size(); ++i) {
                fIndex << i << "\t" << indexArray[i] << std::endl;
            }
            */
            std::ofstream fIndex(m_indexFile.c_str());
            for (std::map<std::string, std::pair<size_t, std::string>>::const_iterator it = m_indexMap.begin(); it != m_indexMap.end(); ++it) {
                fIndex << (it->second).first << "\t" << (it->second).second << std::endl;
            }
            fIndex.close();

        }

        bool operator<(const Field& rhs) const {
            return m_order < rhs.m_order;
        }

        friend std::ostream& operator<<(std::ostream& os, const Field& field) {
            os << "{"
                << "m_index:" << field.m_index << ","
                << "m_order:" << field.m_order << ","
                << "m_save:" << field.m_save << ","
                << "m_save_type:" << (field.m_save_type == 0 ? "index" : "value") << ","
                << "m_dump:" << field.m_dump << ","
                << "m_name:" << field.m_name << ","
                << "m_indexFile:" << field.m_indexFile << ","
                << "m_value:" << field.m_value << ","
                << "m_max:" << field.m_max << ","
                << "m_seq:" << field.m_seq
                << "}";
            return os;
        }
    };


    struct Config {
        std::vector<Field> fieldList;
        std::string redisHost;
        uint16_t redisPort;

        std::string indexFile;
        std::string dbFile;
        std::string keyFile;
        std::string rangeFile;

        size_t mask;
        uint16_t saveType;
        bool trCountry;

        void init(po::variables_map vm) {
            if (vm.count("mask")) {
                mask = vm["mask"].as<size_t>();
            }

            if (vm.count("host")) {
                redisHost = vm["host"].as<std::string>();
            }

            if (vm.count("port")) {
                redisPort = vm["port"].as<uint16_t>();
            }

            if (vm.count("input-file")) {
                dbFile = vm["input-file"].as<std::string>();
            }

            if (vm.count("index-file")) {
                indexFile = vm["index-file"].as<std::string>();
            }

            if (vm.count("range-file")) {
                rangeFile = vm["range-file"].as<std::string>();
            }

            if (vm.count("key-file")) {
                keyFile = vm["key-file"].as<std::string>();
            }

            if (vm.count("save-type")) {
                saveType = vm["save-type"].as<uint16_t>();
            }

            if (vm.count("tr-country")) {
                //std::cout << "tr-country:" << tmp << std::endl;
                uint16_t tmp = vm["tr-country"].as<uint16_t>();
                if (1 == tmp) {
                    trCountry = true;
                }
            }

            if (vm.count("fields")) {
                std::string val = vm["fields"].as<std::string>();
                std::vector<std::string> fields;
                boost::split(fields, val, isDouhao);

                for (size_t i = 0; i < fields.size(); ++i) {
                    Field field;
                    field.init(fields[i]);
                    fieldList.push_back(field);
                }
            }
            std::sort(fieldList.begin(), fieldList.end());

            for (size_t i = 0; i < fieldList.size(); ++i)
                std::cout << fieldList[i]<< std::endl;

            if (vm.count("old-index-dir")) {
                std::string oldIndexDir = vm["old-index-dir"].as<std::string>();
                for (size_t i = 0; i < fieldList.size(); ++i) {
                    fieldList[i].load(oldIndexDir);
                }
            }
        }

    };


    struct Children {
        std::vector<long> data;
    };

    struct IndexArray {
        const size_t static mask = 16;
        std::vector<Children*> regions;

        IndexArray() {
            regions.assign(1 << mask, NULL);
        }
    };

    std::map<std::string, int> stateMap;
    std::map<std::string, int> cityMap;
    std::map<std::string, int> ispMap;


    std::map<std::string, std::string> countryCodeMap;

    IndexArray array;

    struct Field;
    struct Config;

    po::options_description usage("Usage");
    Config cfg;

    void loadCountryCodeMap() {
        countryCodeMap["ZZ"]="ZZZ";
        countryCodeMap["AF"]="AFG";
        countryCodeMap["AX"]="ALA";
        countryCodeMap["AL"]="ALB";
        countryCodeMap["DZ"]="DZA";
        countryCodeMap["AS"]="ASM";
        countryCodeMap["AD"]="AND";
        countryCodeMap["AO"]="AGO";
        countryCodeMap["AI"]="AIA";
        countryCodeMap["AQ"]="ATA";
        countryCodeMap["AG"]="ATG";
        countryCodeMap["AR"]="ARG";
        countryCodeMap["AM"]="ARM";
        countryCodeMap["AW"]="ABW";
        countryCodeMap["AU"]="AUS";
        countryCodeMap["AT"]="AUT";
        countryCodeMap["AZ"]="AZE";
        countryCodeMap["BS"]="BHS";
        countryCodeMap["BH"]="BHR";
        countryCodeMap["BD"]="BGD";
        countryCodeMap["BB"]="BRB";
        countryCodeMap["BY"]="BLR";
        countryCodeMap["BE"]="BEL";
        countryCodeMap["BZ"]="BLZ";
        countryCodeMap["BJ"]="BEN";
        countryCodeMap["BM"]="BMU";
        countryCodeMap["BT"]="BTN";
        countryCodeMap["BO"]="BOL";
        countryCodeMap["BQ"]="BES";
        countryCodeMap["BA"]="BIH";
        countryCodeMap["BW"]="BWA";
        countryCodeMap["BV"]="BVT";
        countryCodeMap["BR"]="BRA";
        countryCodeMap["IO"]="IOT";
        countryCodeMap["BN"]="BRN";
        countryCodeMap["BG"]="BGR";
        countryCodeMap["BF"]="BFA";
        countryCodeMap["BI"]="BDI";
        countryCodeMap["KH"]="KHM";
        countryCodeMap["CM"]="CMR";
        countryCodeMap["CA"]="CAN";
        countryCodeMap["CV"]="CPV";
        countryCodeMap["KY"]="CYM";
        countryCodeMap["CF"]="CAF";
        countryCodeMap["TD"]="TCD";
        countryCodeMap["CL"]="CHL";
        countryCodeMap["CN"]="CHN";
        countryCodeMap["CX"]="CXR";
        countryCodeMap["CC"]="CCK";
        countryCodeMap["CO"]="COL";
        countryCodeMap["KM"]="COM";
        countryCodeMap["CG"]="COG";
        countryCodeMap["CD"]="COD";
        countryCodeMap["CK"]="COK";
        countryCodeMap["CR"]="CRI";
        countryCodeMap["CI"]="CIV";
        countryCodeMap["HR"]="HRV";
        countryCodeMap["CU"]="CUB";
        countryCodeMap["CW"]="CUW";
        countryCodeMap["CY"]="CYP";
        countryCodeMap["CZ"]="CZE";
        countryCodeMap["DK"]="DNK";
        countryCodeMap["DJ"]="DJI";
        countryCodeMap["DM"]="DMA";
        countryCodeMap["DO"]="DOM";
        countryCodeMap["EC"]="ECU";
        countryCodeMap["EG"]="EGY";
        countryCodeMap["SV"]="SLV";
        countryCodeMap["GQ"]="GNQ";
        countryCodeMap["ER"]="ERI";
        countryCodeMap["EE"]="EST";
        countryCodeMap["ET"]="ETH";
        countryCodeMap["FK"]="FLK";
        countryCodeMap["FO"]="FRO";
        countryCodeMap["FJ"]="FJI";
        countryCodeMap["FI"]="FIN";
        countryCodeMap["FR"]="FRA";
        countryCodeMap["GF"]="GUF";
        countryCodeMap["PF"]="PYF";
        countryCodeMap["TF"]="ATF";
        countryCodeMap["GA"]="GAB";
        countryCodeMap["GM"]="GMB";
        countryCodeMap["GE"]="GEO";
        countryCodeMap["DE"]="DEU";
        countryCodeMap["GH"]="GHA";
        countryCodeMap["GI"]="GIB";
        countryCodeMap["GR"]="GRC";
        countryCodeMap["GL"]="GRL";
        countryCodeMap["GD"]="GRD";
        countryCodeMap["GP"]="GLP";
        countryCodeMap["GU"]="GUM";
        countryCodeMap["GT"]="GTM";
        countryCodeMap["GG"]="GGY";
        countryCodeMap["GN"]="GIN";
        countryCodeMap["GW"]="GNB";
        countryCodeMap["GY"]="GUY";
        countryCodeMap["HT"]="HTI";
        countryCodeMap["HM"]="HMD";
        countryCodeMap["VA"]="VAT";
        countryCodeMap["HN"]="HND";
        countryCodeMap["HK"]="HKG";
        countryCodeMap["HU"]="HUN";
        countryCodeMap["IS"]="ISL";
        countryCodeMap["IN"]="IND";
        countryCodeMap["ID"]="IDN";
        countryCodeMap["IR"]="IRN";
        countryCodeMap["IQ"]="IRQ";
        countryCodeMap["IE"]="IRL";
        countryCodeMap["IM"]="IMN";
        countryCodeMap["IL"]="ISR";
        countryCodeMap["IT"]="ITA";
        countryCodeMap["JM"]="JAM";
        countryCodeMap["JP"]="JPN";
        countryCodeMap["JE"]="JEY";
        countryCodeMap["JO"]="JOR";
        countryCodeMap["KZ"]="KAZ";
        countryCodeMap["KE"]="KEN";
        countryCodeMap["KI"]="KIR";
        countryCodeMap["KP"]="PRK";
        countryCodeMap["KR"]="KOR";
        countryCodeMap["KW"]="KWT";
        countryCodeMap["KG"]="KGZ";
        countryCodeMap["LA"]="LAO";
        countryCodeMap["LV"]="LVA";
        countryCodeMap["LB"]="LBN";
        countryCodeMap["LS"]="LSO";
        countryCodeMap["LR"]="LBR";
        countryCodeMap["LY"]="LBY";
        countryCodeMap["LI"]="LIE";
        countryCodeMap["LT"]="LTU";
        countryCodeMap["LU"]="LUX";
        countryCodeMap["MO"]="MAC";
        countryCodeMap["MK"]="MKD";
        countryCodeMap["MG"]="MDG";
        countryCodeMap["MW"]="MWI";
        countryCodeMap["MY"]="MYS";
        countryCodeMap["MV"]="MDV";
        countryCodeMap["ML"]="MLI";
        countryCodeMap["MT"]="MLT";
        countryCodeMap["MH"]="MHL";
        countryCodeMap["MQ"]="MTQ";
        countryCodeMap["MR"]="MRT";
        countryCodeMap["MU"]="MUS";
        countryCodeMap["YT"]="MYT";
        countryCodeMap["MX"]="MEX";
        countryCodeMap["FM"]="FSM";
        countryCodeMap["MD"]="MDA";
        countryCodeMap["MC"]="MCO";
        countryCodeMap["MN"]="MNG";
        countryCodeMap["ME"]="MNE";
        countryCodeMap["MS"]="MSR";
        countryCodeMap["MA"]="MAR";
        countryCodeMap["MZ"]="MOZ";
        countryCodeMap["MM"]="MMR";
        countryCodeMap["NA"]="NAM";
        countryCodeMap["NR"]="NRU";
        countryCodeMap["NP"]="NPL";
        countryCodeMap["NL"]="NLD";
        countryCodeMap["NC"]="NCL";
        countryCodeMap["NZ"]="NZL";
        countryCodeMap["NI"]="NIC";
        countryCodeMap["NE"]="NER";
        countryCodeMap["NG"]="NGA";
        countryCodeMap["NU"]="NIU";
        countryCodeMap["NF"]="NFK";
        countryCodeMap["MP"]="MNP";
        countryCodeMap["NO"]="NOR";
        countryCodeMap["OM"]="OMN";
        countryCodeMap["PK"]="PAK";
        countryCodeMap["PW"]="PLW";
        countryCodeMap["PS"]="PSE";
        countryCodeMap["PA"]="PAN";
        countryCodeMap["PG"]="PNG";
        countryCodeMap["PY"]="PRY";
        countryCodeMap["PE"]="PER";
        countryCodeMap["PH"]="PHL";
        countryCodeMap["PN"]="PCN";
        countryCodeMap["PL"]="POL";
        countryCodeMap["PT"]="PRT";
        countryCodeMap["PR"]="PRI";
        countryCodeMap["QA"]="QAT";
        countryCodeMap["RE"]="REU";
        countryCodeMap["RO"]="ROU";
        countryCodeMap["RU"]="RUS";
        countryCodeMap["RW"]="RWA";
        countryCodeMap["BL"]="BLM";
        countryCodeMap["SH"]="SHN";
        countryCodeMap["KN"]="KNA";
        countryCodeMap["LC"]="LCA";
        countryCodeMap["MF"]="MAF";
        countryCodeMap["PM"]="SPM";
        countryCodeMap["VC"]="VCT";
        countryCodeMap["WS"]="WSM";
        countryCodeMap["SM"]="SMR";
        countryCodeMap["ST"]="STP";
        countryCodeMap["SA"]="SAU";
        countryCodeMap["SN"]="SEN";
        countryCodeMap["RS"]="SRB";
        countryCodeMap["SC"]="SYC";
        countryCodeMap["SL"]="SLE";
        countryCodeMap["SG"]="SGP";
        countryCodeMap["SX"]="SXM";
        countryCodeMap["SK"]="SVK";
        countryCodeMap["SI"]="SVN";
        countryCodeMap["SB"]="SLB";
        countryCodeMap["SO"]="SOM";
        countryCodeMap["ZA"]="ZAF";
        countryCodeMap["GS"]="SGS";
        countryCodeMap["SS"]="SSD";
        countryCodeMap["ES"]="ESP";
        countryCodeMap["LK"]="LKA";
        countryCodeMap["SD"]="SDN";
        countryCodeMap["SR"]="SUR";
        countryCodeMap["SJ"]="SJM";
        countryCodeMap["SZ"]="SWZ";
        countryCodeMap["SE"]="SWE";
        countryCodeMap["CH"]="CHE";
        countryCodeMap["SY"]="SYR";
        countryCodeMap["TW"]="TWN";
        countryCodeMap["TJ"]="TJK";
        countryCodeMap["TZ"]="TZA";
        countryCodeMap["TH"]="THA";
        countryCodeMap["TL"]="TLS";
        countryCodeMap["TG"]="TGO";
        countryCodeMap["TK"]="TKL";
        countryCodeMap["TO"]="TON";
        countryCodeMap["TT"]="TTO";
        countryCodeMap["TN"]="TUN";
        countryCodeMap["TR"]="TUR";
        countryCodeMap["TM"]="TKM";
        countryCodeMap["TC"]="TCA";
        countryCodeMap["TV"]="TUV";
        countryCodeMap["UG"]="UGA";
        countryCodeMap["UA"]="UKR";
        countryCodeMap["AE"]="ARE";
        countryCodeMap["GB"]="GBR";
        countryCodeMap["US"]="USA";
        countryCodeMap["UM"]="UMI";
        countryCodeMap["UY"]="URY";
        countryCodeMap["UZ"]="UZB";
        countryCodeMap["VU"]="VUT";
        countryCodeMap["VE"]="VEN";
        countryCodeMap["VN"]="VNM";
        countryCodeMap["VG"]="VGB";
        countryCodeMap["VI"]="VIR";
        countryCodeMap["WF"]="WLF";
        countryCodeMap["EH"]="ESH";
        countryCodeMap["YE"]="YEM";
        countryCodeMap["ZM"]="ZMB";
        countryCodeMap["ZW"]="ZWE";
    }

    boost::shared_ptr<redis::client> c;

    boost::shared_ptr<redis::client> connect_client()
    {
        /*
        const char* c_host = getenv("REDIS_HOST");
        const char* c_port = getenv("REDIS_PORT");
        std::string host = "localhost";
        uint16_t port = 6379;
        if (c_host)
            host = c_host;
        if (c_port)
            port = boost::lexical_cast<uint16_t>(c_port);
        */
        return boost::shared_ptr<redis::client>( new redis::client(cfg.redisHost, cfg.redisPort) );
    }

    void santizeCarrier(std::string& carrier) {
        // TODO
        if (carrier.compare("\"-\"") == 0 
                || carrier.compare("-") == 0) {
            carrier = "";
        }
        
    }


    void santizeCountry(std::string& country) {
        // "-" or - to ZZ
        if (country.compare("\"-\"") == 0 
                || country.compare("-") == 0) {
            
            country = (cfg.trCountry ? "ZZZ" : "ZZ");
        } else {
            boost::algorithm::trim_if(country, isQuote);
            if (cfg.trCountry) {
                country = countryCodeMap[country];
            }
        }

    }

    void santize(std::string& value) {
        boost::algorithm::trim_if(value, isQuote);
        if (value.compare("\"-\"") == 0 
                || value.compare("-") == 0
                || value.compare("") == 0) {
            value = "unknown";
        }
        
    }


    void setFieldValue(std::vector<Field>& fieldList, std::vector<std::string>& values) {
        for (size_t i = 0; i < fieldList.size(); ++i) {
            Field& field = fieldList[i];
            if (field.m_index < values.size()) {
                field.m_value = values[field.m_index];
                if (field.m_name == "country") {
                    santizeCountry(field.m_value);
                } else if (field.m_name == "mobile_brand") { // carrier
                    santizeCarrier(field.m_value);
                } else {
                    santize(field.m_value);
                }
            }

        }
    }


    void resetFieldValue(std::vector<Field>& fieldList, std::vector<std::string>& values) {
        for (size_t i = 0; i < fieldList.size(); ++i) {
            Field& field = fieldList[i];
            if (field.m_index < values.size())
                field.m_value = "";
        }
    }


    void printFieldValue(std::vector<Field>& fieldList, std::vector<std::string>& values) {
        for (size_t i = 0; i < fieldList.size(); ++i) {
            Field& field = fieldList[i];
            if (field.m_index < values.size()) {
                std::cout << field << std::endl;
            }
        }
    }


    void indexFieldValue(std::vector<Field>& fieldList, std::vector<std::string>& values) {
        for (size_t i = 0; i < fieldList.size(); ++i) {
            Field& field = fieldList[i];
            if (field.m_index < values.size()) {
                field.index();
            }
        }
    }

    std::string concatFieldValue(std::vector<Field>& fieldList, std::vector<std::string>& values) {
        std::stringstream ss;
        for (size_t i = 0; i < fieldList.size(); ++i) {
            Field& field = fieldList[i];
            if (field.m_index < values.size() && field.m_save != 0) {
                if (field.m_name == "country" || field.m_save_type == 1) {
                    ss << "," << field.m_value;
                } else {
                    ss << "," << field.m_seq;
                }

            }
        }
        std::string value = ss.str();
        if (!value.empty()) {
            return ss.str().substr(1);
        }
        return "";
    }



    const uint16_t ASFILE = 1;
    const uint16_t ASREDIS = 2;
    const uint16_t ASFILEREDIS = 3;


    void reassembly2(const std::vector<std::string>& input, std::vector<std::string>& output) {
        size_t len = input.size();
        if (len == 0) return;
        output.clear();
        char s = '\0';
        char e = '\0';
        for (size_t i = 0, k = 0; i < len; ++i) {
            s = input[i][0];
            e = input[i][input[i].length() - 1];

            if (s == '"' && e == '"') {
                if (input[i].length() == 1) {
                    if (output.size() == k) {
                        output.push_back(input[i]);
                        continue;
                    } else {
                        output[k] += "," + input[i];
                    }
                } else {
                    output.push_back(input[i]);
                }

                ++k;
            } else if (s == '"') {
                output.push_back(input[i]);
            } else if (e == '"'){
                output[k] += "," + input[i];
                ++k;
            } else {
                output[k] += "," + input[i];
            }
        }
    }

    void reassembly(const std::vector<std::string>& input, std::vector<std::string>& output) {
        size_t len = input.size();
        if (len == 0) return;
        output.clear();
        char s = '\0';
        char e = '\0';
        bool next = true;
        for (size_t i = 0, k = 0; i < len; ++i) {
            //std::cout << input[i] << " | " << next << std::endl;
            if (next) {
                output.push_back(input[i]);
            } else {
                if (k == output.size()) {
                    output.push_back(input[i]);
                } else {
                    output[k] += "," + input[i];
                }
            }

            s = input[i][0];
            e = input[i][input[i].length() - 1];

            if (s == '"' && e == '"') {
                if (input[i].length() == 1) {
                    if (next == false) {
                        next = true;
                    } else {
                        next = false;
                    }
                } else {
                    // 
                }
            } else if (s == '"') {
                if (next == false) {
                    next = true;
                } else {
                    next = false;
                }
            } else if (e == '"'){
                next = true;
            }
            if (next) {
                ++k;
            }

        }
    }

    void load() {

        if (cfg.fieldList.empty()) {
            std::cerr << "pls set a list of filed to retrive" << std::endl;
            std::cerr << usage << std::endl;
            return;
        }

        if (cfg.dbFile.empty()) {
            std::cerr << "pls set ip database file" << std::endl;
            std::cerr << usage << std::endl;
            return;
        }

        std::ofstream keyFile;
        bool saveKeys = false;
        if (!cfg.keyFile.empty() && (cfg.saveType & ASFILE) == ASFILE) {
            saveKeys = true;
            keyFile.open(cfg.keyFile.c_str(), std::ofstream::app);
        }

        std::ofstream rangeFile;
        bool dumpRange = false;
        if (!cfg.rangeFile.empty()) {
            dumpRange = true;
            rangeFile.open(cfg.rangeFile.c_str(), std::ofstream::app);
        }


        std::cout << "start loading....." << std::endl;

        std::string line;
        std::ifstream fDB(cfg.dbFile.c_str());

        long lineNumber = 0;
        long preEnd = -1;
        int preReg = 0;
        int regionIndex = 0;
        long offset = 0;
        int region = 0;
        long start = 0;
        long end = 0;

        bool dumpRedis = false;
        if ((cfg.saveType & ASREDIS) != 0) {
            dumpRedis = true;
            c = connect_client();
        }

        bool dumpFile = false;
        if ((cfg.saveType & ASFILE) != 0) {
            dumpFile = true;
        }

        std::vector<std::string> tempValues;
        std::vector<std::string> values;
//        std::map<size_t, size_t> nfRecords;
        size_t len = 0;
        char last = '\0';
        while (getline(fDB, line)) {
            lineNumber++;

            if ((lineNumber & 8191) == 0) {
                std::cout << "loading " << lineNumber << std::endl;
            }

            len = line.length();
            if (0 == len) {
                std::cerr << "empty line " << lineNumber << std::endl;
                continue;
            }
            last = line[len - 1];
            if (last == '\r' || last == '\n') {
                line.resize(len - 1);
            }
            len = line.length();
            if (0 == len) {
                std::cerr << "empty line after resize " << lineNumber << std::endl;
                continue;
            }


            boost::split(tempValues, line, isDouhao);

            reassembly(tempValues, values);
            boost::algorithm::trim_if(values[IndexUnit::start], isQuote);
            start = boost::lexical_cast<long>(values[IndexUnit::start]);
            boost::algorithm::trim_if(values[IndexUnit::end], isQuote);
            end = boost::lexical_cast<long>(values[IndexUnit::end]);

            setFieldValue(cfg.fieldList, values);

            //std::cerr << "NF " << values.size() << std::endl;

            /*
            if (dumpRange) {
                rangeFile << "start=" << start
                    << ",end=" << end
                    << ",preEnd=" << preEnd;
            }

            std::cout << "start=" << start
                << ",end=" << end
                << ",preEnd=" << preEnd
                << std::endl;
            */

            if (preEnd < 0 || start > preEnd)// 有重叠的IP进行去除，保留不重复数据
            {
                region = (int)(end >> IndexArray::mask);
                if (region != preReg) {
                    preReg = region;
                    regionIndex = 0;
                    offset = ((long)region) * (1 << IndexArray::mask);
                    array.regions[region] = new Children();
                }
                array.regions[region]->data.push_back(end - offset);
                preEnd = end;

                /*
                rangeFile << ",region=" << region
                    << ",regionIndex=" << regionIndex
                    << ",offset=" << offset;
                    */

                indexFieldValue(cfg.fieldList, values);

                // save into redis
                std::stringstream ssKey;
                //ssKey << "i" << region << "_" << regionIndex;
                ssKey << region << "_" << regionIndex;

                std::string value = concatFieldValue(cfg.fieldList, values);

                //std::cout << ssKey.str() << ":" << value << std::endl;

                if (dumpFile) {
                    keyFile << ssKey.str() << ':' << value << std::endl;
                }

                if (dumpRedis) {
                    c->set(ssKey.str(), value);
                }

                ++regionIndex;

            }

            if (dumpRange) {
                rangeFile << start
                    << "," << end
                    << "," << values[IndexUnit::country]
                    << std::endl;
            }
        }

        std::cout << "loading " << lineNumber << std::endl;

        fDB.close();
        if ((cfg.saveType & ASFILE) != 0) {
            keyFile.close();
        }

        if (dumpRange) {
            rangeFile.close();
        }
    }


    void dump(std::string& file, IndexArray& indexArray) {
        std::ofstream fIndex(file.c_str());
        long offset = 0;
        for (size_t i = 0; i < indexArray.regions.size(); ++i) {
            Children* region = indexArray.regions[i];
            if (NULL != region && region->data.size() > 0) {
                fIndex << i << ":" << region->data.size() << ":" << offset << std::endl;
                if (region->data.size() >= 1)
                    fIndex << region->data[0];
                for (size_t j = 1; j < region->data.size(); ++j) {
                    fIndex << "," << region->data[j];
                }
                fIndex << std::endl;
            }
            offset += (1 << IndexArray::mask);
        }
        fIndex.close();
    }

    void dump(std::string file, std::map<std::string, size_t> indexMap) {
    }

}


int main(int argc, char* argv[])
{
    using namespace yeahmobi;


    // Declare a group of options that will be
    // allowed only on command line
    usage.add_options()
        ("help", "produce help message")
        ("mask,m", po::value<size_t>()->default_value(16), "mode mask to distribute ip")
        ("tr-country", po::value<uint16_t>()->default_value(0), "translate 2 country code to 3")
        ("host,h", po::value<std::string>()->default_value("localhost"), "host of redis server")
        ("port,p", po::value<uint16_t>()->default_value(6379), "port of redis server")
        ("fields", po::value<std::string>(), "list of fields[index:outorder:save:dumpindex:name:indexfile] \n to save into redis: \"1:1:1:0:isp:isp.db,2:2:1:1:city:city.db\"")
        ("input-file", po::value<std::string>(), "raw ip database")
        ("index-file", po::value<std::string>(), "file to save ip index")
        ("key-file", po::value<std::string>(), "file to save ip keys")
        ("range-file", po::value<std::string>(), "file to save ip range & country code")
        ("save-type", po::value<uint16_t>(), "db to save: 1: file, 2: redis, 3: file && redis")
        ("old-index-dir", po::value<std::string>(), "index file of last version")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, usage), vm);
    po::notify(vm);

    cfg.init(vm);

    loadCountryCodeMap();

    load();

    dump(cfg.indexFile, array);

    for (size_t i = 0; i < cfg.fieldList.size(); ++i) {
        Field field = cfg.fieldList[i];
        field.dump();

    }

    std::cout << XXXX << std::endl;


    /*
    const char* dbFile = "/data1/sdk/ip/test.CSV"; // if not given, looking at the classpath
    const char* indexFile = "IPRangeIndex.db"; // if not given, looking at the classpath
    const char* stateFile = "stateIndex.db"; // if not given, looking at the classpath
    const char* cityFile = "cityIndex.db"; // if not given, looking at the classpath
    const char* ispFile = "ispIndex.db"; // if not given, looking at the classpath
    if (argc == 6) {
        dbFile = argv[1];
        indexFile = argv[2];
        stateFile = argv[3]; // if not given, looking at the classpath
        cityFile = argv[4]; // if not given, looking at the classpath
        ispFile = argv[5]; // if not given, looking at the classpath
    }

    load(dbFile);

    dump(indexFile, array);
    dump(stateFile, stateMap);
    dump(cityFile, cityMap);
    dump(ispFile, ispMap);
    */

    return 0;

}

