#pragma once

#include <variant>
#include <iostream>
#include <string>


enum class DataError {
    BrokenPipe,
    ConnectionReset,
    ConnectionAborted,
    UnknownError,
};

// T cannot be data error
template <typename T, typename = std::enable_if_t<!std::is_same_v<T, DataError>>>
struct Result {
private:
    std::variant<T, DataError> result_;

public:
    Result() = default;
    Result(const T& value) : result_(value) {}
    Result(const DataError& error) : result_(error) {}

    bool is_err() const {
        return std::holds_alternative<DataError>(result_);
    }

    bool is_ok() const {
        return std::holds_alternative<T>(result_);
    }

    // get value, throw exception if error
    const T& deref() const {
        if (is_err()) 
            throw std::runtime_error("Error: " + std::to_string(static_cast<int>(std::get<DataError>(result_))));
        else 
            return std::get<T>(result_);
    }

    // operator *
    const T& operator*() const {
        return this->deref();
    }

    DataError msg() const {
        return std::get<DataError>(result_);
    }

    // reload the operator
    friend std::ostream& operator<<(std::ostream& os, const Result& result) {
        if (result.is_ok()) {
            os << "Success: " << result.deref();
        } else {
            os << "Error: " << static_cast<int>(result.msg());
        }
        return os;
    }

};