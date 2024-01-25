#include <iostream>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "yaml-cpp/yaml.h"
#include "const_def.h"

int main() {
    auto &&config_path = kStaticRoot + kConfigPath;
    auto &&config = YAML::LoadFile(config_path.c_str());

    auto &&stocks =  config["subscribe_stock"];

    for (auto &&stock : stocks) {
        std::cout << stock.as<std::string>() << std::endl;
    }

    return 0;
}
