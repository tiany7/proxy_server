#include "data_resolver.h"

int main() {
    
    DataResolver<int> dr;
    auto handle = dr.MockSubscribeData(3);
    for (int i = 0; i < 10; ++i) {
        Result<int> result(i);
        while (!handle->pop(result));
        std::cout << *result << std::endl;
    }
    dr.Stop();
    return 0;
}