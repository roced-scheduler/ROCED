# ![ROCED][logo]
**R**apid **O**n-Demand **C**loud-**e**nabled **D**eployment is a tool which can interface with different batch systems (Torque, HTCondor) and cloud sites (Eucalyptus, OpenNebula, OpenStack, Amazon EC2, etc.).
It monitors demand of computing resources in the batch system(s) and dynamically manages VMs (starting and terminating them) on different cloud sites.

## Design

ROCED periodically runs a management cycle, where it performs three steps:
* Monitor a batch system's queue and determine demand for machines
* Boot machines
* Integrate booted machines into batch system

![Visualisation of management cycle][workflow]

ROCED consists of five components; everything except the core has a modular structure, in order to offer a maximum of flexibility. Users can freely combine different adapters to fulfill their requirement or even write their own. ROCED needs at least one of each component to be in any way useful and we advice to use *Requirement Adapter* and *Integration Adapter* for the same batch system.
* **Core**
* **Requirement Adapters**  
  Monitor batch system(s) to determine the demand for machines.
* **Site Adapters**  
Request machines at cloud site(s)
* **Integration Adapters**  
(Dis-)Integrate running machines from/into batch system(s)
 * **Broker**  
Balance demand across different cloud sites, depending on different metrics (e.g.: cost)

![Visualisation of modular components][design]

## Requirements/Installation
* Python 2.7 or 3.5
    * Python 2 *requires* the [future](http://python-future.org/) package
    * Various adapters have system/site dependant packages.  
We follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/#imports) guideline when listing module imports, that way you can easily identify the needed modules for each adapter.  
E.g. *novaclient* is required to interface with OpenStack.
* Batch system

# Contributors
ROCED was developed at the *Institut für Experimentelle Kernphysik* at the *Karlsruhe Institute of Technology*.  
Further information can be found in the documentation folder.
# License
    ROCED is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    ROCED is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with ROCED.  If not, see <http://www.gnu.org/licenses/>.

[logo]: https://cdn.rawgit.com/roced-scheduler/ROCED/master/doc/roced_logo.svg
[design]: https://cdn.rawgit.com/roced-scheduler/ROCED/master/doc/roced_design.svg
[workflow]: https://cdn.rawgit.com/roced-scheduler/ROCED/master/doc/roced_workflow.svg
