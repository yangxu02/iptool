#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/timer/timer.hpp>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <boost/ref.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <iostream>
#include <fstream>

#include <boost/atomic.hpp>

#include "redisclient.h"
#include "bounded_buffer.hpp"

namespace po = boost::program_options;

namespace iptool {

    boost::shared_ptr<redis::client> c;

    struct Config {
        std::string redisHost;
        uint16_t redisPort;

        std::string inputFile;

        uint16_t producerThreadCount;
        uint16_t consumerThreadCount;
        bool test;

        //boost::lockfree::queue<std::string> queue;

        void init(po::variables_map vm) {

            if (vm.count("host")) {
                redisHost = vm["host"].as<std::string>();
            }

            if (vm.count("port")) {
                redisPort = vm["port"].as<uint16_t>();
            }

            if (vm.count("input-file")) {
                inputFile = vm["input-file"].as<std::string>();
            }

            if (vm.count("thread-count")) {
                consumerThreadCount = vm["thread-count"].as<uint16_t>();
            }

            test = false;
            if (vm.count("test-enabled")) {
                test = vm["test-enabled"].as<bool>();
            }

            producerThreadCount = 1;
        }
    };

    struct Config;
    Config cfg;



    const boost::algorithm::detail::is_any_ofF<char> isQuote = boost::is_any_of("\"");
    const boost::algorithm::detail::is_any_ofF<char> isComma = boost::is_any_of(":");
    const boost::algorithm::detail::is_any_ofF<char> isDouhao = boost::is_any_of(",");
    const boost::algorithm::detail::is_any_ofF<char> isTab = boost::is_any_of("\t");


    boost::atomic_long producer_count(0);
    boost::atomic_long consumer_count(0);
    boost::posix_time::ptime producer_last = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::ptime consumer_last = boost::posix_time::microsec_clock::local_time();

    bounded_buffer<std::string> queue(1024);

    std::string posion = "eof";

    void producer()
    {
        const char* file = cfg.inputFile.c_str();
        std::ifstream fIndex(file);
        std::string line;
        while (getline(fIndex, line)) {
            queue.push_front(line);
            ++producer_count;
        }
        fIndex.close();
        for (uint16_t i = 0; i < cfg.consumerThreadCount; ++i) {
            queue.push_front(posion);
        }

    }

    boost::atomic<bool> done (false);
    void consumer(void)
    {
        std::string line;
        std::vector<std::string> values;
        bool test = cfg.test;
        while (true) {
            queue.pop_back(&line);
            if (posion.compare(line) == 0) break;
            boost::split(values, line, isComma);
            ++consumer_count;
            if (!test)
                c->set(values[0], values[1]);
            else
                std::cerr << consumer_count << "->" << line << std::endl;
        }
    }

    void stats() {
        long i = 0;
        while (!done) {
            boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
            std::cout << "produced: "  << producer_count
                << ", consumed: " << consumer_count
                << ", elapsed: " << ++i << "s"
                << std::endl;
        }
    }

    po::options_description usage("Usage");


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

}

int main(int argc, char* argv[])
{
    using namespace iptool;

    // Declare a group of options that will be
    // allowed only on command line
    usage.add_options()
        ("help", "produce help message")
        ("host,h", po::value<std::string>()->default_value("localhost"), "host of redis server")
        ("port,p", po::value<uint16_t>()->default_value(6379), "port of redis server")
        ("input-file", po::value<std::string>(), "ip database")
        ("thread-count", po::value<uint16_t>(), "threads to update redis")
        ("test-enabled", po::value<bool>(), "test mode:just echo each line")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, usage), vm);
    po::notify(vm);

    cfg.init(vm);

    if (!cfg.test) {
        c = connect_client();
    }

    boost::thread_group producer_threads, consumer_threads, stats_thread;

    for (int i = 0; i != cfg.producerThreadCount; ++i)
        producer_threads.create_thread(producer);

    for (int i = 0; i != cfg.consumerThreadCount; ++i)
        consumer_threads.create_thread(consumer);

    stats_thread.create_thread(stats);

    producer_threads.join_all();

    consumer_threads.join_all();
    done = true;
    stats_thread.join_all();

    std::cout << "produced " << producer_count << " objects." << std::endl;
    std::cout << "consumed " << consumer_count << " objects." << std::endl;

    return 0;

}

