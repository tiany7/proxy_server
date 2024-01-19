#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <iostream>

int main() {
    try {
        // Create a shared memory object
        boost::interprocess::shared_memory_object shm(
            boost::interprocess::create_only, "MySharedMemory", boost::interprocess::read_write);

        // Set the size of the shared memory
        shm.truncate(1024);

        // Map the shared memory into the process address space
        boost::interprocess::mapped_region region(shm, boost::interprocess::read_write);

        // Access and modify the shared memory
        char* data = static_cast<char*>(region.get_address());
        std::strcpy(data, "Hello, Boost Shared Memory!");

        std::cout << "Data in shared memory: " << data << std::endl;

        // ... Do other operations with shared memory ...

        // Remove the shared memory object when done
        boost::interprocess::shared_memory_object::remove("MySharedMemory");
    } catch (boost::interprocess::interprocess_exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return 0;
}
