#ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H
#define CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H


#include <climits>
#include <vector>

namespace cds {
    namespace container {


        class BrownHelgaKtree {
        public:
            int k;
            struct UpdateStep{};

            struct Clean: public UpdateStep{};

            struct Node {
                std::vector<int> keys;
            };

            struct Leaf : public Node {
                int keyCount;

                Leaf(int keyCount) {
                    this->keyCount = keyCount;
                }
            };

            struct InternalNode : public Node {
                std::vector<Node *> c_nodes;
                UpdateStep pending;

                InternalNode(){
                  pending = Clean();
                };
            };

            struct ReplaceFlag: public UpdateStep{
                const Node l;
                const Node p;
                const Node newChild;
                const int pindex;
            };

            struct PruneFlag: public UpdateStep{
                const Node l;
                const Node p;
                const Node gp;
                const UpdateStep ppending;
                const int gpindex;
            };


            struct Mark: public UpdateStep{
                const PruneFlag pending;
            };


            InternalNode *root;
        protected:



        public:
            BrownHelgaKtree() {
                k = 4;
                root = new InternalNode();

                for (int i = 0; i < k - 1; ++i) {
                    root->keys.push_back(INT_MAX);
                }

                InternalNode *leftmostChild = new InternalNode();

                for (int i = 0; i < k - 1; ++i) {
                    leftmostChild->keys.push_back(INT_MAX);
                }

                for (int i = 0; i < k; ++i) {
                    leftmostChild->c_nodes.push_back(new Leaf(i));
                }

                root->c_nodes.push_back(leftmostChild);

                for (int i = 1; i < k; ++i) {
                    root->c_nodes.push_back(new Leaf(0));
                }

            }

            BrownHelgaKtree(int k) {
                this->k = k;
            }

            ~BrownHelgaKtree() {}

        };

    }
}

int main() {
    auto *brownHelgaKtree = new cds::container::BrownHelgaKtree();
    auto based = (cds::container::BrownHelgaKtree::InternalNode*)brownHelgaKtree->root->c_nodes.at(0);
    return 0;
}

#endif // #ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H