/* get_hash
 * get hash value using cmh algorithm
 * hash = get_hash(key);
*/

#include "cmph/cmph.h"
#include <string>
#include "mex.hpp"
#include "mexAdapter.hpp"

using namespace matlab::data;
using matlab::mex::ArgumentList;

class MexFunction : public matlab::mex::Function {
     matlab::data::ArrayFactory factory;
public:
    void operator()(ArgumentList outputs, ArgumentList inputs) {
//         checkArguments(outputs, inputs);
        FILE* mphf_fd = fopen("1M.keys.mph", "r");
        cmph_t* hash = cmph_load(mphf_fd);
        std::string key = std::to_string((unsigned int)inputs[0][0]);
        unsigned int id = cmph_search(hash, key.c_str(), (cmph_uint32)strlen(key.c_str()));
        matlab::data::TypedArray<double> ans = factory.createArray<double>({ 1,1 });
        ans[0][0] = id;
        outputs[0] = std::move(ans);
    }

    void checkArguments(ArgumentList outputs, ArgumentList inputs) {
        // Get pointer to engine
        std::shared_ptr<matlab::engine::MATLABEngine> matlabPtr = getEngine();

        // Get array factory
        ArrayFactory factory;

        // Check first input argument
        if (inputs[0].getType() != ArrayType::DOUBLE ||
            inputs[0].getType() == ArrayType::COMPLEX_DOUBLE ||
            inputs[0].getNumberOfElements() != 1)
        {
            matlabPtr->feval(u"error",
                0,
                std::vector<Array>({ factory.createScalar("First input must scalar double") }));
        }

        // // Check second input argument
        // if (inputs[1].getType() != ArrayType::DOUBLE ||
        //     inputs[1].getType() == ArrayType::COMPLEX_DOUBLE)
        // {
        //     matlabPtr->feval(u"error",
        //         0,
        //         std::vector<Array>({ factory.createScalar("Input must double array") }));
        // }
        // Check number of outputs
        if (outputs.size() > 1) {
            matlabPtr->feval(u"error",
                0,
                std::vector<Array>({ factory.createScalar("Only one output is returned") }));
        }
    }
};

