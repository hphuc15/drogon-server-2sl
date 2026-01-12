#include "api_data.h"
#include <json/json.h>

using namespace api;

namespace api
{
    void data::postData(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&callback)
    {
        auto jsonStr = req->getJsonObject();
        if (!jsonStr)
        {
            Json::Value status;
            status["Error"] = "Invalid JSON";
            HttpResponsePtr resp = HttpResponse::newHttpJsonResponse(status);
            callback(resp);
            return;
        }

        int sensor_id = (*jsonStr)["sensor_id"].asInt();
        float light = (*jsonStr)["light"].asFloat();

        auto client = drogon::app().getDbClient("default");

        // std::cout << "DB CLIENT PTR = " << client.get() << std::endl; // debug

        std::string SQL_insert = "INSERT INTO bh1750 (sensor_id, light) VALUES (?, ?)";
        client->execSqlAsync(
            SQL_insert,
            [=](const drogon::orm::Result &result)
            {
                std::cout << result.affectedRows() << " inserted!" << std::endl;
                Json::Value status;
                status["Status"] = "Received";
                // Debug: check data by client
                status["sensor_id"] = sensor_id;
                status["light"] = light;
                // std::cout << sensor_id << std::endl; // debug
                // spdlog::info("Light: {}", sensor_id);
                auto response = HttpResponse::newHttpJsonResponse(status);
                callback(response);
            },
            [=](const drogon::orm::DrogonDbException &e)
            {
                std::cerr << "error: " << e.base().what() << std::endl;
                Json::Value status;
                status["Error"] = e.base().what();
                auto response = HttpResponse::newHttpJsonResponse(status);
                callback(response);
            },
            sensor_id,
            light);

        // test: curl -X POST http://localhost:5000/api/data/postData -H "Content-Type: application/json" -d "{\"sensor_id\":105,\"light\":192}"
    }

    void data::getData(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&callback)
    {
        // Get Database client
        auto client = drogon::app().getDbClient("default");

        // Get data filter parameters
        DataFilter_t data_filter = {
            .sensor_id = req->getParameter("sensor_id"),
            .date = req->getParameter("date"),
            .sort = {req->getParameter("sort")},
            .level = req->getParameter("level")};
        data_filter.to_valid_sort(); // Delete null string in sort

        // Get offset parameter (mới thêm)
        int offset = 0;
        try
        {
            auto offset_str = req->getParameter("offset");
            if (!offset_str.empty())
            {
                offset = std::stoi(offset_str);
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error parsing offset: " << e.what() << std::endl;
            offset = 0;
        }

        // Create query string
        std::string SQL_str = "SELECT sensor_id, date, time, light FROM bh1750 WHERE 1=1";

        // Check filter parameter
        if (data_filter.is_empty())
        {
            SQL_str.append(" ORDER BY date DESC, time DESC LIMIT 30 OFFSET ");
            SQL_str.append(std::to_string(offset));
            SQL_str.append(";");
        }
        else
        {
            // ============ DATA FILTER BLOCK ==============
            // Search
            if (!data_filter.sensor_id.empty())
            {
                std::string SQL_search;
                SQL_search.append(" AND sensor_id = ");
                SQL_search.append(data_filter.sensor_id);
                SQL_str.append(SQL_search);
            }
            if (!data_filter.date.empty())
            {
                std::string SQL_search;
                SQL_search.append(" AND date = \"");
                SQL_search.append(data_filter.date);
                SQL_search.append("\"");
                SQL_str.append(SQL_search);
            }

            if (!data_filter.level.empty())
            {
                std::string SQL_search;
                if (data_filter.level == "low") // [0, 1000)
                {
                    SQL_search.append(" AND light >= 0 AND light < 1000");
                }
                else if (data_filter.level == "medium") // [1000, 10000)
                {
                    SQL_search.append(" AND light >= 1000 AND light < 10000");
                }
                else if (data_filter.level == "high") // [10000, max)
                {
                    SQL_search.append(" AND light >= 10000");
                }
                SQL_str.append(SQL_search);
            }

            // Sort
            if (!data_filter.sort.empty())
            {
                int s_comma_count = data_filter.sort.size() - 1;
                std::string SQL_sort = " ORDER BY";
                for (auto param : data_filter.sort)
                {
                    SQL_sort.append(" ");
                    SQL_sort.append(param);
                    SQL_sort.append(" DESC");
                    while (s_comma_count != 0)
                    {
                        SQL_sort.append(",");
                        s_comma_count--;
                    }
                }
                SQL_str.append(SQL_sort);
            }

            // Thêm LIMIT và OFFSET (sửa dòng này)
            SQL_str.append(" LIMIT 30 OFFSET ");
            SQL_str.append(std::to_string(offset));
            SQL_str.append(";");
        }

        // ============== Execute ==================
        client->execSqlAsync(
            SQL_str,
            [=](const drogon::orm::Result &result)
            {
                std::cout << result.size() << " rows selected" << std::endl;

                std::vector<std::map<std::string, std::string>> dataList;
                for (auto row : result)
                {
                    std::map<std::string, std::string> rowData;
                    rowData["sensor_id"] = std::to_string(row["sensor_id"].as<int>());
                    rowData["date"] = row["date"].as<std::string>();
                    rowData["time"] = row["time"].as<std::string>();
                    rowData["light"] = std::to_string(row["light"].as<float>());
                    dataList.push_back(rowData);
                }

                HttpViewData viewData;
                viewData.insert("data", dataList);
                // Truyền thêm offset cho template
                viewData.insert("current_offset", offset);
                // Truyền số lượng bản ghi hiện tại
                viewData.insert("current_count", dataList.size());

                auto resp = drogon::HttpResponse::newHttpViewResponse("data.csp", viewData);
                callback(resp);
            },
            [=](const drogon::orm::DrogonDbException &e)
            {
                std::cerr << "error: " << e.base().what() << std::endl;
                Json::Value status;
                status["Error"] = e.base().what();
                auto response = HttpResponse::newHttpJsonResponse(status);
                callback(response);
            });
    }

    void data::exportCsv(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&callback)
    {
        auto client = drogon::app().getDbClient("default");

        DataFilter_t data_filter = {
            .sensor_id = req->getParameter("sensor_id"),
            .date = req->getParameter("date"),
            .sort = {req->getParameter("sort")},
            .level = req->getParameter("level")};
        data_filter.to_valid_sort();

        std::string SQL_exportCsv = "SELECT sensor_id, date, time, light FROM bh1750 WHERE 1=1";

        if (data_filter.is_empty())
        {
            SQL_exportCsv.append(" ORDER BY date DESC, time DESC;");
        }
        else
        {
            if (!data_filter.sensor_id.empty())
            {
                SQL_exportCsv.append(" AND sensor_id = ");
                SQL_exportCsv.append(data_filter.sensor_id);
            }
            if (!data_filter.date.empty())
            {
                SQL_exportCsv.append(" AND date = \"");
                SQL_exportCsv.append(data_filter.date);
                SQL_exportCsv.append("\"");
            }

            if (!data_filter.level.empty())
            {
                if (data_filter.level == "low") // [0, 1000)
                {
                    SQL_exportCsv.append(" AND light >= 0 AND light < 1000");
                }
                else if (data_filter.level == "medium") // [1000, 10000)
                {
                    SQL_exportCsv.append(" AND light >= 1000 AND light < 10000");
                }
                else if (data_filter.level == "high") // [10000, max)
                {
                    SQL_exportCsv.append(" AND light >= 10000");
                }
            }

            if (!data_filter.sort.empty())
            {
                int s_comma_count = data_filter.sort.size() - 1;
                SQL_exportCsv.append(" ORDER BY");
                for (auto param : data_filter.sort)
                {
                    SQL_exportCsv.append(" ");
                    SQL_exportCsv.append(param);
                    SQL_exportCsv.append(" DESC");
                    while (s_comma_count != 0)
                    {
                        SQL_exportCsv.append(",");
                        s_comma_count--;
                    }
                }
            }
        }

        client->execSqlAsync(
            SQL_exportCsv,
            [=](const drogon::orm::Result &result)
            {
                std::string csv = "sensor_id,date,time,light\n";

                for (auto const &row : result)
                {
                    csv += row["sensor_id"].as<std::string>() + "," +
                           row["date"].as<std::string>() + "," +
                           row["time"].as<std::string>() + "," +
                           row["light"].as<std::string>() + "\n";
                }

                auto resp = drogon::HttpResponse::newHttpResponse();
                resp->setContentTypeCode(CT_TEXT_CSV);
                resp->addHeader(
                    "Content-Disposition",
                    "attachment; filename=sensor_data.csv");
                resp->setBody(csv);

                callback(resp);
            },

            [=](const drogon::orm::DrogonDbException &e)
            {
                std::cerr << "error: " << e.base().what() << std::endl;
                Json::Value status;
                status["Error"] = e.base().what();
                auto response = HttpResponse::newHttpJsonResponse(status);
                callback(response);
            });
    }
}