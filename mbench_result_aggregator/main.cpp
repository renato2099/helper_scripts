/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include "sqlite3.h"
#include <iostream>
#include <boost/format.hpp>
#include <map>
#include <vector>
#include <cmath>

#define sqlOk(code) assertSql(code, __FILE__, __LINE__)

void assertSql(int code, const char* file, int line) {
    if (code != SQLITE_OK) {
            auto msg = (boost::format("ERROR (%1%:%2%): %3%") % file % line % sqlite3_errstr(code)).str();
            throw std::runtime_error(msg.c_str());
        }
}

uint   tpFactor   = 200;
double onemillion = 1000000.0;

template<class T>
double tp(const std::string& txName, const std::vector<T>& res, long min, long max) {
    double tp = double(res.size())/(double((max - min))/onemillion);
    if (txName == "BatchOp" || txName == "Populate") {
        return floor(tpFactor*tp);
    }
    return tp;
}

float avg(const std::vector<float>& res) {
    float r = 0.0;
    for (auto rt : res) {
        r += rt;
    }
    return r/float(res.size());
}

std::pair<float, float> avgstdev(const std::vector<float>& res) {
    auto a = avg(res);
    float r = 0.0;
    for (auto rt : res) {
        r += rt * rt;
    }
    float avgsq = r/float(res.size());
    float s = sqrtf(avgsq - a*a);
    return std::make_pair(a, s);
}

float quantile(int percentage, const std::vector<float>& resp) {
    return resp[percentage * resp.size() / 100];
}

void printResults(const char* file) {
    sqlite3* db;
    sqlOk(sqlite3_open(file, &db));
    sqlite3_stmt* stmt;
    sqlOk(sqlite3_prepare_v2(db, "SELECT min(start), max(end) FROM results", -1, &stmt, nullptr));
    int s;
    long min, max;
    while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
        if (s == SQLITE_ERROR) throw std::runtime_error(sqlite3_errmsg(db));
        min = sqlite3_column_int64(stmt, 0);
        max = sqlite3_column_int64(stmt, 1);
    }
    sqlite3_finalize(stmt);
    auto fetchQuery = boost::str(boost::format(
        "SELECT tx, rt/%1% "
        "FROM results "
        //"WHERE start > %2% AND end < %3% AND success LIKE 'true' "
    ) % onemillion);// % min % max);
    sqlOk(sqlite3_prepare_v2(db, fetchQuery.c_str(), -1, &stmt, nullptr));
    std::map<std::string, std::vector<float>> results;
    while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
        if (s != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(db));
        auto str = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        std::string tx = "";
        if (str)
            tx = str;
        float rt = sqlite3_column_double(stmt, 1);
        results[tx].push_back(rt);
    }
    for (auto& p : results) {
        std::sort(p.second.begin(), p.second.end());
    }
    sqlite3_finalize(stmt);
    for (auto& p : results) {
        float a, s;
        std::tie(a, s) = avgstdev(p.second);
        auto fmt = boost::format("%1%,%2%,%3%,%4%,%5%,%6%,%7%,%8%,%9%,%10%,%11%,%12%,%13%")
            % p.first
            % tp(p.first, p.second, min, max)
            % a
            % s
            % quantile(50, p.second)
            % quantile(99, p.second)
            % quantile(95, p.second)
            % quantile(1, p.second)
            % quantile(5, p.second)
            % quantile(25, p.second)
            % quantile(75, p.second)
            % p.second.front()
            % p.second.back()
            ;
        std::cout << fmt << std::endl;
    }
    sqlite3_close(db);
}

int main(int argc, const char* argv[]) {
    if (argc < 2) {
        std::cerr << "USAGE: resaggr sqlitedb\n";
        return 1;
    }
    sqlOk(sqlite3_config(SQLITE_CONFIG_SINGLETHREAD));
    std::cout << "Transaction,Throughput,Response Time, Standard Deviation,Median,99% Quantile,95% Quantile,1% Quantile,5% Quantile,25% Quantile,75% Quantile,Min,Max\n";
    for (int i = 1; i < argc; ++i) {
        printResults(argv[i]);
    }
}
