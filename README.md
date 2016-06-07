# ROCED

**R**apid **O**n-Demand **C**loud-**e**nabled **D**eployment is a tool which can interface with different batch systems (Torque, HTCondor) and cloud sites (Eucalyptus, OpenNebula, OpenStack, Amazon EC2, etc.).
It monitors demand of computing resources in the batch system(s) and dynamically manages VMs (starting and terminating them) on different cloud sites.

## Requirements/Installation
* Python (Supported: 2.7 & 3.5)
  * python-future
  * python-lxml
  * Optional site dependant packages, for example python-novaclient to interface with OpenStack.
* Batch system

# Contributors
ROCED was developed at the Institut für Experimentelle Kernphysik at the Karlsruhe-Institut of Technology.

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
