#pragma once

#include <drogon/HttpController.h>
#include <drogon/HttpAppFramework.h>

using namespace drogon;

namespace api
{
  class data : public drogon::HttpController<data>
  {
  public:
    METHOD_LIST_BEGIN
    // use METHOD_ADD to add your custom processing function here;
    // METHOD_ADD(data::get, "/{2}/{1}", Get); // path is /api/data/{arg2}/{arg1}
    // METHOD_ADD(data::your_method_name, "/{1}/{2}/list", Get); // path is /api/data/{arg1}/{arg2}/list
    // ADD_METHOD_TO(data::your_method_name, "/absolute/path/{1}/{2}/list", Get); // path is /absolute/path/{arg1}/{arg2}/list

    METHOD_ADD(data::postData, "/postData", Post);
    METHOD_ADD(data::getData, "/getData", Get);
    METHOD_ADD(data::exportCsv, "/exportCsv", Get);

    METHOD_LIST_END
    // your declaration of processing function maybe like this:
    // void get(const HttpRequestPtr& req, std::function<void (const HttpResponsePtr &)> &&callback, int p1, std::string p2);
    // void your_method_name(const HttpRequestPtr& req, std::function<void (const HttpResponsePtr &)> &&callback, double p1, int p2) const;

    struct DataFilter_t
    {
      std::string sensor_id;
      std::string date;
      std::vector<std::string> sort;
      std::string level; // low, medium, high

      // Pagination
      std::string limit;
      std::string offset;

      void to_valid_sort()
      {
        sort.erase(
            std::remove_if(sort.begin(), sort.end(), [](const std::string &s)
                           { return s.empty(); }),
            sort.end());
      }

      bool is_empty()
      {
        return sensor_id.empty() && date.empty() && sort.empty() && level.empty();
      }
    };

    void postData(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&callback);
    void getData(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&callback);
    void exportCsv(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&callback);
  };
}
